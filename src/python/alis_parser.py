# This file replaces the legacy KornShell script: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
# It re-engineers the custom templating and parsing functionality into Python.

import re
import datetime
import os

class AlisParser:
    """
    A Python-based parser and templating utility designed to replicate the
    functionality of the h_alis_parser.ksh KornShell script.
    It handles variable assignments, list definitions, list expansions,
    file inclusions, and timestamp generation within a template string.
    """

    def __init__(self, file_resolver=None):
        """
        Initializes the parser with an empty context and an optional file resolver.

        Args:
            file_resolver (callable, optional): A function that takes a filepath
                                                (str) and returns its content (str).
                                                Defaults to reading from the local filesystem.
        """
        self.context = {}
        # Default file resolver assumes local files, but can be overridden for GCS etc.
        self.file_resolver = file_resolver if file_resolver else self._default_file_resolver
        self.errors = []

    def _default_file_resolver(self, filepath):
        """
        Default file resolver that reads content from the local filesystem.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Included file not found: {filepath}")
        with open(filepath, 'r') as f:
            return f.read()

    def _clean_content(self, content):
        """
        Removes KornShell-style comments (#) and empty lines from the content.
        """
        lines = content.splitlines()
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                cleaned_lines.append(line)
        return "\n".join(cleaned_lines)

    def _parser_filattrib(self, text, placeholder, value):
        """
        Replaces occurrences of a specific placeholder (e.g., '<VAR>') with a value.
        """
        return text.replace(placeholder, str(value))

    def _process_single_list_item_template(self, template_str, item_dict, is_single=False, is_first=False, is_last=False):
        """
        Processes a template block for a single list item, applying item-specific attributes
        and handling <FIRST>, <MIDDLE>, <END>, <SINGLE> conditional tags.
        """
        temp_output = template_str

        # Apply attribute replacements for current item (e.g., <ATTR1> -> item_dict['ATTR1'])
        for attr, value in item_dict.items():
            temp_output = self._parser_filattrib(temp_output, f'<{attr}>', value)
        
        # Determine which conditional block to keep
        chosen_content = ""
        if is_single:
            match = re.search(r'<SINGLE>(.*?)</SINGLE>', temp_output, re.DOTALL)
            if match:
                chosen_content = match.group(1)
        elif is_first:
            match = re.search(r'<FIRST>(.*?)</FIRST>', temp_output, re.DOTALL)
            if match:
                chosen_content = match.group(1)
        elif is_last:
            match = re.search(r'<END>(.*?)</END>', temp_output, re.DOTALL)
            if match:
                chosen_content = match.group(1)
        else: # Middle item
            match = re.search(r'<MIDDLE>(.*?)</MIDDLE>', temp_output, re.DOTALL)
            if match:
                chosen_content = match.group(1)
        
        return chosen_content

    def parse_template(self, template_content, initial_context=None):
        """
        Parses the given template content, applying variable substitutions,
        list expansions, includes, and timestamp generations.

        Args:
            template_content (str): The raw template string to process.
            initial_context (dict, optional): A dictionary of initial variables
                                              and list definitions.

        Returns:
            str: The processed template content.
        """
        self.context.update(initial_context if initial_context else {})
        cleaned_content = self._clean_content(template_content)

        processed_lines_for_second_pass = []
        in_listdef_mode = False
        current_list_name = None
        current_list_attrs = []
        current_list_items = []

        # --- First Pass: Process variable assignments and LISTDEF blocks ---
        # This pass populates the context with global variables and structured list data.
        for line in cleaned_content.splitlines():
            # Variable assignment: <VAR=VALUE>
            var_assign_match = re.match(r'^<(\w+)=([^>]*)>$', line)
            if var_assign_match:
                var_name = var_assign_match.group(1)
                var_value = var_assign_match.group(2)
                self.context[var_name] = var_value
                continue

            # LISTDEF start: <LISTDEF NAME ATTR1 ATTR2 ...>
            listdef_start_match = re.match(r'^<LISTDEF (\w+)\s*(.*)>$', line)
            if listdef_start_match:
                current_list_name = listdef_start_match.group(1)
                attrs_str = listdef_start_match.group(2).strip()
                current_list_attrs = re.split(r'\s+', attrs_str) if attrs_str else []
                current_list_items = []
                in_listdef_mode = True
                continue

            # LISTDEF end: </LISTDEF>
            if line == '</LISTDEF>':
                if in_listdef_mode:
                    self.context[current_list_name] = current_list_items
                    in_listdef_mode = False
                    current_list_name = None
                    current_list_attrs = []
                    current_list_items = []
                else:
                    self.errors.append("Syntax Error: </LISTDEF> without preceding <LISTDEF>")
                continue

            # Inside LISTDEF: Item definition (e.g., 'VALUE1 VALUE2')
            if in_listdef_mode and current_list_name:
                item_values = re.split(r'\s+', line.strip())
                item_dict = {}
                for i, attr in enumerate(current_list_attrs):
                    item_dict[attr] = item_values[i] if i < len(item_values) else ''
                current_list_items.append(item_dict)
                continue

            # If the line is not a definition, keep it for the second pass
            processed_lines_for_second_pass.append(line)

        # Re-join lines for the second pass (actual content transformations)
        output = "\n".join(processed_lines_for_second_pass)

        # --- Second Pass: Apply transformations ---

        # 1. Variable substitution: <VAR> (after all variables are defined)
        # This needs to be iterated to handle variables that might be defined using other variables.
        # For simplicity, a single pass. If complex chained variable defs are needed,
        # a more sophisticated dependency graph or iterative replacement might be necessary.
        for var_name, var_value in self.context.items():
            output = self._parser_filattrib(output, f'<{var_name}>', var_value)


        # 2. LIST block expansion: <LIST NAME>...</LIST>
        # Pattern captures the entire block, the list name, and the body.
        list_block_pattern = re.compile(r'(?s)<LIST\s+(\w+).*?>(.*?)</LIST>') # (?s) makes '.' match newlines

        while True:
            match = list_block_pattern.search(output)
            if not match:
                break
            
            full_list_block_text = match.group(0)
            list_name = match.group(1)
            list_body_template = match.group(2) # The content between <LIST> and </LIST>

            items = self.context.get(list_name, [])

            if not isinstance(items, list):
                self.errors.append(f"Context for LIST '{list_name}' is not a valid list.")
                output = output.replace(full_list_block_text, f"<!-- ERROR: LIST '{list_name}' not found or invalid -->")
                continue

            expanded_list_content_parts = []
            if len(items) == 0:
                # Remove the entire list block if the list is empty
                pass
            elif len(items) == 1:
                expanded_list_content_parts.append(
                    self._process_single_list_item_template(list_body_template, items[0], is_single=True)
                )
            else:
                for i, item_dict in enumerate(items):
                    if i == 0:
                        expanded_list_content_parts.append(
                            self._process_single_list_item_template(list_body_template, item_dict, is_first=True)
                        )
                    elif i == len(items) - 1:
                        expanded_list_content_parts.append(
                            self._process_single_list_item_template(list_body_template, item_dict, is_last=True)
                        )
                    else:
                        expanded_list_content_parts.append(
                            self._process_single_list_item_template(list_body_template, item_dict)
                        )
            
            output = output.replace(full_list_block_text, "".join(expanded_list_content_parts))

        # 3. Handle Includes: <INCLUDE FILE='path'/>
        # Recursively processes included files.
        include_pattern = re.compile(r"<INCLUDE\s+FILE=['\"](.*?)['\"]\s*/>")
        while True:
            match = include_pattern.search(output)
            if not match:
                break
            filepath = match.group(1)
            try:
                included_content = self.file_resolver(filepath)
                # Recursively parse included content, passing current context
                # Note: This simple recursion doesn't handle infinite include loops.
                parsed_included_content = self.parse_template(included_content, self.context)
                output = output.replace(match.group(0), parsed_included_content)
            except FileNotFoundError as e:
                self.errors.append(f"Error including file {filepath}: {e}")
                output = output.replace(match.group(0), f"<!-- ERROR: File '{filepath}' not found -->")
            except Exception as e:
                self.errors.append(f"Error parsing included file {filepath}: {e}")
                output = output.replace(match.group(0), f"<!-- ERROR: Parsing included file '{filepath}' failed -->")

        # 4. Handle Timestamps: <TIMESTAMP FORMAT='...' OFFSET='...'/>
        timestamp_pattern = re.compile(r"<TIMESTAMP(?:\s+FORMAT=['\"](.*?)['\"])?(?:\s+OFFSET=['\"](.*?)['\"])?\s*/>")
        # Find all timestamps and replace them. Use re.sub to handle multiple occurrences.
        
        # Using re.sub with a replacer function to avoid issues with modifying string while iterating matches
        def replace_timestamp(match):
            fmt = match.group(1) if match.group(1) else "%Y-%m-%d %H:%M:%S" # Default format
            offset_str = match.group(2)
            
            current_time = datetime.datetime.now()
            
            if offset_str:
                offset_match = re.match(r'([+-]?\d+)([dhms])', offset_str)
                if offset_match:
                    value = int(offset_match.group(1))
                    unit = offset_match.group(2)
                    if unit == 'd':
                        current_time += datetime.timedelta(days=value)
                    elif unit == 'h':
                        current_time += datetime.timedelta(hours=value)
                    elif unit == 'm':
                        current_time += datetime.timedelta(minutes=value)
                    elif unit == 's':
                        current_time += datetime.timedelta(seconds=value)
                    else:
                        self.errors.append(f"Invalid timestamp offset unit: {unit}")
                else:
                    self.errors.append(f"Invalid timestamp offset format: {offset_str}")

            return current_time.strftime(fmt)

        output = timestamp_pattern.sub(replace_timestamp, output)

        return output

# Example Usage (optional, for demonstration/testing within the file)
if __name__ == "__main__":
    # Create some dummy files for INCLUDE directives
    with open("temp_include1.txt", "w") as f:
        f.write("# This is an included file\n")
        f.write("Included Var: <INC_VAR>\n")
        f.write("Another included timestamp: <TIMESTAMP FORMAT='%Y%m%d' OFFSET='+1d'/>\n")

    def custom_file_resolver(filepath):
        print(f"Resolving file: {filepath}")
        return AlisParser._default_file_resolver(None, filepath) # Use default logic for local files

    parser = AlisParser(file_resolver=custom_file_resolver)

    template = """
# Main Template
<GLOBAL_VAR=Hello World>
<ANOTHER_VAR=123>

This is a global variable: <GLOBAL_VAR>
And another: <ANOTHER_VAR>

<LISTDEF USERS NAME AGE CITY>
  Alice 30 NewYork
  Bob 24 London
  Charlie 35 Paris
</LISTDEF>

--- List of Users ---
<LIST USERS>
<FIRST>First User: <NAME> (<AGE>) from <CITY></FIRST>
<MIDDLE>Middle User: <NAME> (<AGE>) from <CITY></MIDDLE>
<END>Last User: <NAME> (<AGE>) from <CITY></END>
<SINGLE>Only User: <NAME> (<AGE>) from <CITY></SINGLE>
</LIST>
--- End List ---

<LISTDEF EMPTY_LIST>
</LISTDEF>
Empty List Test:
<LIST EMPTY_LIST>
    This should not appear.
</LIST>

<LISTDEF SINGLE_USER_LIST NAME>
  David
</LISTDEF>
--- Single User List ---
<LIST SINGLE_USER_LIST>
<FIRST>First/Single: <NAME></FIRST>
<SINGLE>This is the ONLY user: <NAME></SINGLE>
<END>Last/Single: <NAME></END>
</LIST>
--- End Single User List ---

Today's date: <TIMESTAMP FORMAT='%Y-%m-%d'/>
Timestamp with offset: <TIMESTAMP FORMAT='%H:%M:%S' OFFSET='-1h'/>
Future timestamp: <TIMESTAMP OFFSET='+7d'/>

--- Included Content ---
<INCLUDE FILE='temp_include1.txt'/>
--- End Included Content ---

A final variable check: <GLOBAL_VAR>
"""

    processed_output = parser.parse_template(template, initial_context={'INC_VAR': 'IncludedValue'})
    print("\n--- Processed Output ---")
    print(processed_output)
    
    if parser.errors:
        print("\n--- Parser Errors ---")
        for error in parser.errors:
            print(f"- {error}")

    # Clean up dummy files
    os.remove("temp_include1.txt")

    print("\n--- Test with only one list item ---")
    parser_single = AlisParser()
    template_single_list = """
<LISTDEF PEOPLE NAME>
  Eva
</LISTDEF>
<LIST PEOPLE>
<FIRST>First Person: <NAME></FIRST>
<MIDDLE>Middle Person: <NAME></MIDDLE>
<END>Last Person: <NAME></END>
<SINGLE>Single Person: <NAME></SINGLE>
</LIST>
"""
    processed_single_list = parser_single.parse_template(template_single_list)
    print(processed_single_list)

    print("\n--- Test with multiple list items ---")
    parser_multi = AlisParser()
    template_multi_list = """
<LISTDEF PEOPLE NAME>
  Frank
  Grace
  Henry
</LISTDEF>
<LIST PEOPLE>
<FIRST>First Person: <NAME></FIRST>
<MIDDLE>Middle Person: <NAME></MIDDLE>
<END>Last Person: <NAME></END>
<SINGLE>Single Person: <NAME></SINGLE>
</LIST>
"""
    processed_multi_list = parser_multi.parse_template(template_multi_list)
    print(processed_multi_list)