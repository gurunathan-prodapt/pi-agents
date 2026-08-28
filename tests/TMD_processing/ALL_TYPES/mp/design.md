GRAPH: tmpjaasp8qn

=== SOURCES ===
[Determine team virtuality CCOS (Lookup contains only visible teams)] kind=select
  SELECT --+ use_hash(vir,tea,abt) 
stichtag, tea.sdm_team_id FROM 
CCR$TA_F_TEAMSICHTBARKEIT vir,
ccr$ta_s_sdm_team tea,
ccr$ta_s_sdm_abteilung abt
WHERE team_sichtbarkeitstyp_id = 10
AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
AND   tea.sdm_team_id = vir.sdm_team_id
AND   abt.sdm_abteilung_id = tea.sdm_abteilung_id
ORDER BY stichtag, sdm_team_id
[tos_cancellations.dat] kind=file
  tos_cancellations.dat
[x_tos_measures] kind=file
  x_tos_measures.dat
[Create Lkp_teamvirt_ccos] kind=select
  SELECT --+ use_hash(vir,tea,abt) 
stichtag, tea.sdm_team_id FROM 
CCR$TA_F_TEAMSICHTBARKEIT vir,
ccr$ta_s_sdm_team tea,
ccr$ta_s_sdm_abteilung abt
WHERE team_sichtbarkeitstyp_id = 10
AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
AND   tea.sdm_team_id = vir.sdm_team_id
AND   abt.sdm_abteilung_id = tea.sdm_abteilung_id
ORDER BY stichtag, sdm_team_id

=== LOOKUPS ===
[Lkp_teamvirt_ccos] file=UNKNOWN — not extracted; supply physical source manually
  keys=UNKNOWN
  schema=UNKNOWN

=== TRANSFORMS ===
[Reformat-1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_stornos :: in.mea_1;
  out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
  first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
else
  NULL;
end;
[Reformat-2] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_stornos :: in.mea_1;
  out.sdm_team_id :: in.sdm_team_id;
end;
[Reformat-1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_produkte :: in.mea_1;
  out.tcn_offer_product_id :: string_concat(string_lrtrim(in.tos_offer_id), "~", string_lrtrim(in.tcn_product_id));
  out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
  first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
else
  NULL;
end;
[Reformat-2] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_produkte :: in.mea_1;
  out.tcn_offer_product_id :: string_concat(string_lrtrim(in.tos_offer_id), "~", string_lrtrim(in.tcn_product_id));
  out.sdm_team_id :: in.sdm_team_id;
end;
[Reformat-1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_angebote :: if (in.tos_mea_group_name == 'QUOTES') in.mea_1;
  out.subventionen :: if (in.tos_mea_group_name == 'QUOTES') string_replace(in.mea_2, '.', ',');
  out.anzahl_vertraege :: if (in.tos_mea_group_name == 'CONTRACTS') in.mea_1;
  out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
  first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
else
  NULL;
end;
[Reformat-2] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.anzahl_angebote :: if (in.tos_mea_group_name == 'QUOTES') in.mea_1;
  out.subventionen :: if (in.tos_mea_group_name == 'QUOTES') string_replace(in.mea_2, '.', ',');
  out.anzahl_vertraege :: if (in.tos_mea_group_name == 'CONTRACTS') in.mea_1;
  out.sdm_team_id :: in.sdm_team_id;
end;

=== FILTERS ===
[Filter by MEA Group]
  tos_mea_group_name == 'CANCELLATIONS'
[Filter to previous week greatest]
  stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))
[Filter by MEA Group]
  tos_mea_group_name == 'PRODUCTS'
[Filter to previous week greatest]
  stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))
[Filter by MEA Group]
  tos_mea_group_name == 'CONTRACTS' \|\| tos_mea_group_name == 'QUOTES'
[Filter to previous week greatest]
  stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))

=== DB JOINS ===
  (none extracted)

=== SORTS AND DEDUPS ===
  (none extracted)

=== TARGETS ===
[Create Lkp_teamvirt_ccos] kind=file table_or_path=lkp_team_virt_ccos.dat
[Lkp_teamvirt_ccos] kind=file table_or_path=lkp_team_virt_ccos.dat
[tos_cancellations.dat] kind=file table_or_path=tos_cancellations.dat
[x_tos_measures] kind=file table_or_path=x_tos_measures.dat
[tos_cancellations_wk.dat] kind=file table_or_path=tos_cancellations_wk.dat
[tos_products.dat] kind=file table_or_path=tos_products.dat
[tos_products_wk.dat] kind=file table_or_path=tos_products_wk.dat
[tos_quotes_contracts.dat] kind=file table_or_path=tos_quotes_contracts.dat
[tos_quotes_contracts_wk.dat] kind=file table_or_path=tos_quotes_contracts_wk.dat
[output_file_54945] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_cancellations.dat
[output_file_66167] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_cancellations_wk.dat
[output_file_96060] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_products.dat
[output_file_104553] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_products_wk.dat
[output_file_134671] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts.dat
[output_file_143176] kind=file table_or_path=file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts_wk.dat

=== EDGES (source-to-target wiring) ===
  Replicate --> Reformat-1
  Filter to previous week greatest --> Reformat-2
  Determine team virtuality CCOS (Lookup contains only visible teams) --> Create Lkp_teamvirt_ccos
  Reformat-2 --> tos_quotes_contracts_wk.dat
  Reformat-2 --> tos_cancellations_wk.dat
  Filter by MEA Group --> Replicate
  Reformat-2 --> tos_products_wk.dat
  Reformat-1 --> tos_products.dat
  Reformat-1 --> tos_cancellations.dat
  Reformat-1 --> tos_quotes_contracts.dat
  x_tos_measures --> node_110


# PySpark Migration Design Document: tmpjaasp8qn

---

## 1. GRAPH OVERVIEW
The graph `tmpjaasp8qn` processes transactional measurement data related to cancellations, products, quotes, and contracts. It reads historical measure details from an input flat file (`x_tos_measures.dat`), applies business filtering and lookup transformations based on team visibility rules, and splits the data into two distinct lifecycles: a standard view (containing all processed records mapped with resolved virtual team IDs) and a weekly rolled-over view (filtered to dates prior to the current business week, retaining original team IDs). The output targets are distinct flat files structured by measure type (Cancellations, Products, and Quotes/Contracts) for both regular and weekly intervals.

---

## 2. SOURCES

### Source 1: Determine team virtuality CCOS (Lookup contains only visible teams)
* **Kind**: Select Query
* **SQL Expression**:
  ```sql
  SELECT --+ use_hash(vir,tea,abt) 
  stichtag, tea.sdm_team_id FROM 
  CCR$TA_F_TEAMSICHTBARKEIT vir,
  ccr$ta_s_sdm_team tea,
  ccr$ta_s_sdm_abteilung abt
  WHERE team_sichtbarkeitstyp_id = 10
  AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
  AND   tea.sdm_team_id = vir.sdm_team_id
  AND   abt.sdm_abteilung_id = tea.sdm_abteilung_id
  ORDER BY stichtag, sdm_team_id
  ```

### Source 2: x_tos_measures
* **Kind**: File
* **Physical Source Path**: `x_tos_measures.dat`

---

## 3. TRANSFORMS

### Transform 1: Reformat-1 (Cancellations Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_stornos :: in.mea_1;
    out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
    first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
  else
    NULL;
  end;
  ```
* **Description**: Map cancellations count from measure field 1, and override the team ID by querying the visible teams lookup.

### Transform 2: Reformat-2 (Cancellations Weekly Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_stornos :: in.mea_1;
    out.sdm_team_id :: in.sdm_team_id;
  end;
  ```
* **Description**: Map cancellations count from measure field 1, preserving the original team ID without lookup modification.

### Transform 3: Reformat-1 (Products Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_produkte :: in.mea_1;
    out.tcn_offer_product_id :: string_concat(string_lrtrim(in.tos_offer_id), "~", string_lrtrim(in.tcn_product_id));
    out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
    first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
  else
    NULL;
  end;
  ```
* **Description**: Map products count, concatenate clean offer and product identifiers, and resolve virtual team ID.

### Transform 4: Reformat-2 (Products Weekly Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_produkte :: in.mea_1;
    out.tcn_offer_product_id :: string_concat(string_lrtrim(in.tos_offer_id), "~", string_lrtrim(in.tcn_product_id));
    out.sdm_team_id :: in.sdm_team_id;
  end;
  ```
* **Description**: Map products count and concatenated product offer key, bypassing the team ID lookup.

### Transform 5: Reformat-1 (Quotes & Contracts Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_angebote :: if (in.tos_mea_group_name == 'QUOTES') in.mea_1;
    out.subventionen :: if (in.tos_mea_group_name == 'QUOTES') string_replace(in.mea_2, '.', ',');
    out.anzahl_vertraege :: if (in.tos_mea_group_name == 'CONTRACTS') in.mea_1;
    out.sdm_team_id :: if (is_defined(in.sdm_team_id)) 
    first_defined(lookup("Lkp_teamvirt_ccos", in.stichtag, in.sdm_team_id).sdm_team_id, '')
  else
    NULL;
  end;
  ```
* **Description**: Extracts quotes and contracts specific metrics (and formatting subventions with comma separators) and overrides target team visibility identifier.

### Transform 6: Reformat-2 (Quotes & Contracts Weekly Stream)
* **Type**: `reformat`
* **Expression**:
  ```begin
    out.* :: in.*;
    out.anzahl_angebote :: if (in.tos_mea_group_name == 'QUOTES') in.mea_1;
    out.subventionen :: if (in.tos_mea_group_name == 'QUOTES') string_replace(in.mea_2, '.', ',');
    out.anzahl_vertraege :: if (in.tos_mea_group_name == 'CONTRACTS') in.mea_1;
    out.sdm_team_id :: in.sdm_team_id;
  end;
  ```
* **Description**: Extracts quote and contract specific counts and parameters, keeping original team ID mapping.

---

## 4. IN-MEMORY LOOKUPS

### Lookup 1: Lkp_teamvirt_ccos
* **Lookup Name**: `Lkp_teamvirt_ccos`
* **Join Key(s)**: `stichtag`, `sdm_team_id`
* **Columns Returned**: `sdm_team_id`
* **Resolution**: Resolved via the inline query pipeline "Determine team virtuality CCOS".

---

## 5. FILTERS (select_expr)

### Filter 1: Filter by MEA Group (Cancellations)
* **Label**: `Filter by MEA Group`
* **Expression**: `tos_mea_group_name == 'CANCELLATIONS'`
* **Effect**: Routes cancellation measures only.

### Filter 2: Filter to previous week greatest (Cancellations Weekly)
* **Label**: `Filter to previous week greatest`
* **Expression**: `stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))`
* **Effect**: Filters data down to the previous week's boundary relative to execution time.

### Filter 3: Filter by MEA Group (Products)
* **Label**: `Filter by MEA Group`
* **Expression**: `tos_mea_group_name == 'PRODUCTS'`
* **Effect**: Routes product measures only.

### Filter 4: Filter to previous week greatest (Products Weekly)
* **Label**: `Filter to previous week greatest`
* **Expression**: `stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))`
* **Effect**: Filters product data down to the previous week's boundary.

### Filter 5: Filter by MEA Group (Quotes & Contracts)
* **Label**: `Filter by MEA Group`
* **Expression**: `tos_mea_group_name == 'CONTRACTS' || tos_mea_group_name == 'QUOTES'`
* **Effect**: Routes contract and quote measures only.

### Filter 6: Filter to previous week greatest (Quotes & Contracts Weekly)
* **Label**: `Filter to previous week greatest`
* **Expression**: `stichtag < datetime_add(now(),((datetime_day_of_week(now()) - 2) *-1))`
* **Effect**: Filters quote and contract data down to the previous week's boundary.

---

## 6. OUTPUT TARGETS

### Target 1: Create Lkp_teamvirt_ccos / lkp_team_virt_ccos.dat
* **Label**: `Create Lkp_teamvirt_ccos` / `Lkp_teamvirt_ccos`
* **Kind**: `file`
* **Table or path**: `lkp_team_virt_ccos.dat`
* **SQL Statement**: (Built directly from "Determine team virtuality CCOS" select query source)

### Target 2: tos_cancellations.dat
* **Label**: `output_file_54945`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_cancellations.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_cancellations.dat] — SQL not extracted; supply manually

### Target 3: tos_cancellations_wk.dat
* **Label**: `output_file_66167`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_cancellations_wk.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_cancellations_wk.dat] — SQL not extracted; supply manually

### Target 4: tos_products.dat
* **Label**: `output_file_96060`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_products.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_products.dat] — SQL not extracted; supply manually

### Target 5: tos_products_wk.dat
* **Label**: `output_file_104553`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_products_wk.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_products_wk.dat] — SQL not extracted; supply manually

### Target 6: tos_quotes_contracts.dat
* **Label**: `output_file_134671`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts.dat] — SQL not extracted; supply manually

### Target 7: tos_quotes_contracts_wk.dat
* **Label**: `output_file_143176`
* **Kind**: `file`
* **Table or path**: `file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts_wk.dat`
* **SQL Statement**: # REVIEW: [file] to [file:$CCR_AI_DAT_FILE_DIR/tos_quotes_contracts_wk.dat] — SQL not extracted; supply manually

---

## 7. DB JOINS
*(No DB Joins / parameterized live lookups extracted)*

---

## 8. BUSINESS SUMMARY
* **Lookup Generation**: Executes visibility mapping logic query using a hash-join on tables representing teams (`ccr$ta_s_sdm_team`), departments (`ccr$ta_s_sdm_abteilung`), and team visibility mappings (`CCR$TA_F_TEAMSICHTBARKEIT`). This output is preserved as `Lkp_teamvirt_ccos` to cleanse records downstream.
* **Stream Splitting & Measure Segmentation**: Measures from `x_tos_measures.dat` are categorized into three parallel pathways: Cancellations (`CANCELLATIONS`), Products (`PRODUCTS`), and Quotes/Contracts (`QUOTES` or `CONTRACTS`).
* **Active Pipeline Execution (Standard Output)**: Standard pathways join the stream against the visibility lookup `Lkp_teamvirt_ccos` matching on `stichtag` and `sdm_team_id`. Unmatched or null visibility combinations map the `sdm_team_id` to `""`.
* **Historical Processing (Weekly Output)**: A relative-week mathematical check selects all measures whose report date (`stichtag`) is strictly prior to Monday of the current active calendar week. These records bypass lookup overrides, maintaining their raw original `sdm_team_id` field.
* **Target File Storage**: Finalized sets are serialized as distinct structured tables across six explicit storage boundaries segmenting data category and historical depth.

---

## PYSPARK PSEUDOCODE OUTLINE

```python
# -----------------------------------------------------------------------------
# STEP 1: Generate Team Virtuality Lookup View
# -----------------------------------------------------------------------------
df_team_virt_src = spark.sql("""
    SELECT 
      vir.stichtag, 
      tea.sdm_team_id 
    FROM 
      BIGQUERY_SOURCE_DS.CCR$TA_F_TEAMSICHTBARKEIT vir
      INNER JOIN BIGQUERY_SOURCE_DS.ccr$ta_s_sdm_team tea 
        ON tea.sdm_team_id = vir.sdm_team_id
      INNER JOIN BIGQUERY_SOURCE_DS.ccr$ta_s_sdm_abteilung abt 
        ON abt.sdm_abteilung_id = tea.sdm_abteilung_id
    WHERE 
      vir.team_sichtbarkeitstyp_id = 10
      AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
    ORDER BY 
      vir.stichtag, 
      tea.sdm_team_id
""")
df_team_virt_src.createOrReplaceTempView("lkp_team_virt_ccos")
write_to_bq(df_team_virt_src, "lkp_team_virt_ccos")

# -----------------------------------------------------------------------------
# STEP 2: Read Main Input Stream
# -----------------------------------------------------------------------------
df_x_tos_measures = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.x_tos_measures")
df_x_tos_measures.createOrReplaceTempView("src_measures")

# -----------------------------------------------------------------------------
# STEP 3: Cancellations Stream
# -----------------------------------------------------------------------------

# Step 3.1: Apply base Cancellation filter
df_cancellations_base = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM src_measures
    WHERE tos_mea_group_name = 'CANCELLATIONS'
""")
df_cancellations_base.createOrReplaceTempView("cancellations_base")

# Step 3.2: Reformat-1 Standard Cancellations (Lookup Join)
df_tos_cancellations = spark.sql("""
    SELECT 
      in.stichtag,
      in.tos_mea_group_name,
      in.mea_1,
      in.mea_2,
      in.tos_offer_id,
      in.tcn_product_id,
      in.mea_1 AS anzahl_stornos,
      CASE 
        WHEN in.sdm_team_id IS NOT NULL THEN coalesce(lkp.sdm_team_id, '')
        ELSE NULL 
      END AS sdm_team_id
    FROM cancellations_base in
    LEFT JOIN lkp_team_virt_ccos lkp 
      ON in.stichtag = lkp.stichtag 
     AND in.sdm_team_id = lkp.sdm_team_id
""")
df_tos_cancellations.createOrReplaceTempView("tos_cancellations_reformatted")
write_to_bq(df_tos_cancellations, "tos_cancellations")

# Step 3.3: Weekly boundary Filter for Cancellations
df_cancellations_wk_filtered = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM cancellations_base
    WHERE stichtag < date_add(current_timestamp(), CAST(((dayofweek(current_timestamp()) - 2) * -1) AS INT))
""")
df_cancellations_wk_filtered.createOrReplaceTempView("cancellations_wk_filtered")

# Step 3.4: Reformat-2 Weekly Cancellations
df_tos_cancellations_wk = spark.sql("""
    SELECT 
      stichtag,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id,
      mea_1 AS anzahl_stornos,
      sdm_team_id
    FROM cancellations_wk_filtered
""")
df_tos_cancellations_wk.createOrReplaceTempView("tos_cancellations_wk_reformatted")
write_to_bq(df_tos_cancellations_wk, "tos_cancellations_wk")


# -----------------------------------------------------------------------------
# STEP 4: Products Stream
# -----------------------------------------------------------------------------

# Step 4.1: Apply base Product filter
df_products_base = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM src_measures
    WHERE tos_mea_group_name = 'PRODUCTS'
""")
df_products_base.createOrReplaceTempView("products_base")

# Step 4.2: Reformat-1 Standard Products (Lookup Join)
df_tos_products = spark.sql("""
    SELECT 
      in.stichtag,
      in.tos_mea_group_name,
      in.mea_1,
      in.mea_2,
      in.tos_offer_id,
      in.tcn_product_id,
      in.mea_1 AS anzahl_produkte,
      concat(trim(in.tos_offer_id), '~', trim(in.tcn_product_id)) AS tcn_offer_product_id,
      CASE 
        WHEN in.sdm_team_id IS NOT NULL THEN coalesce(lkp.sdm_team_id, '')
        ELSE NULL 
      END AS sdm_team_id
    FROM products_base in
    LEFT JOIN lkp_team_virt_ccos lkp 
      ON in.stichtag = lkp.stichtag 
     AND in.sdm_team_id = lkp.sdm_team_id
""")
df_tos_products.createOrReplaceTempView("tos_products_reformatted")
write_to_bq(df_tos_products, "tos_products")

# Step 4.3: Weekly boundary Filter for Products
df_products_wk_filtered = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM products_base
    WHERE stichtag < date_add(current_timestamp(), CAST(((dayofweek(current_timestamp()) - 2) * -1) AS INT))
""")
df_products_wk_filtered.createOrReplaceTempView("products_wk_filtered")

# Step 4.4: Reformat-2 Weekly Products
df_tos_products_wk = spark.sql("""
    SELECT 
      stichtag,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id,
      mea_1 AS anzahl_produkte,
      concat(trim(tos_offer_id), '~', trim(tcn_product_id)) AS tcn_offer_product_id,
      sdm_team_id
    FROM products_wk_filtered
""")
df_tos_products_wk.createOrReplaceTempView("tos_products_wk_reformatted")
write_to_bq(df_tos_products_wk, "tos_products_wk")


# -----------------------------------------------------------------------------
# STEP 5: Quotes & Contracts Stream
# -----------------------------------------------------------------------------

# Step 5.1: Apply base Quotes/Contracts filter
df_quotes_contracts_base = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM src_measures
    WHERE tos_mea_group_name = 'CONTRACTS' OR tos_mea_group_name = 'QUOTES'
""")
df_quotes_contracts_base.createOrReplaceTempView("quotes_contracts_base")

# Step 5.2: Reformat-1 Standard Quotes/Contracts (Lookup Join)
df_tos_quotes_contracts = spark.sql("""
    SELECT 
      in.stichtag,
      in.tos_mea_group_name,
      in.mea_1,
      in.mea_2,
      in.tos_offer_id,
      in.tcn_product_id,
      CASE WHEN in.tos_mea_group_name = 'QUOTES' THEN in.mea_1 END AS anzahl_angebote,
      CASE WHEN in.tos_mea_group_name = 'QUOTES' THEN replace(in.mea_2, '.', ',') END AS subventionen,
      CASE WHEN in.tos_mea_group_name = 'CONTRACTS' THEN in.mea_1 END AS anzahl_vertraege,
      CASE 
        WHEN in.sdm_team_id IS NOT NULL THEN coalesce(lkp.sdm_team_id, '')
        ELSE NULL 
      END AS sdm_team_id
    FROM quotes_contracts_base in
    LEFT JOIN lkp_team_virt_ccos lkp 
      ON in.stichtag = lkp.stichtag 
     AND in.sdm_team_id = lkp.sdm_team_id
""")
df_tos_quotes_contracts.createOrReplaceTempView("tos_quotes_contracts_reformatted")
write_to_bq(df_tos_quotes_contracts, "tos_quotes_contracts")

# Step 5.3: Weekly boundary Filter for Quotes/Contracts
df_quotes_contracts_wk_filtered = spark.sql("""
    SELECT 
      stichtag,
      sdm_team_id,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id
    FROM quotes_contracts_base
    WHERE stichtag < date_add(current_timestamp(), CAST(((dayofweek(current_timestamp()) - 2) * -1) AS INT))
""")
df_quotes_contracts_wk_filtered.createOrReplaceTempView("quotes_contracts_wk_filtered")

# Step 5.4: Reformat-2 Weekly Quotes/Contracts
df_tos_quotes_contracts_wk = spark.sql("""
    SELECT 
      stichtag,
      tos_mea_group_name,
      mea_1,
      mea_2,
      tos_offer_id,
      tcn_product_id,
      CASE WHEN tos_mea_group_name = 'QUOTES' THEN mea_1 END AS anzahl_angebote,
      CASE WHEN tos_mea_group_name = 'QUOTES' THEN replace(mea_2, '.', ',') END AS subventionen,
      CASE WHEN tos_mea_group_name = 'CONTRACTS' THEN mea_1 END AS anzahl_vertraege,
      sdm_team_id
    FROM quotes_contracts_wk_filtered
""")
df_tos_quotes_contracts_wk.createOrReplaceTempView("tos_quotes_contracts_wk_reformatted")
write_to_bq(df_tos_quotes_contracts_wk, "tos_quotes_contracts_wk")
```

### 1. JOB DEPENDENCIES
* **Downstream Dependencies**:
  * **`DW.DWH_ALL_TYPES_MASTER`** (Not yet migrated): Consumes the outputs generated by this job. Once migrated to BigQuery, the final target BigQuery tables produced by this PySpark script can be queried directly or integrated via cross-DAG datasets.
  * **`TMD_processing/ALL_TYPES/run/all_types_graph.ksh`** (Not yet migrated): The wrapper script that orchestrates and executes this Ab Initio graph. On the target platform, the logic from this wrapper script will be converted into a Cloud Composer (Airflow) DAG task (using `DataprocServerlessStartBatchOperator`) to invoke the migrated PySpark script (`all_types_graph.py`).
* **Wiring on Target Platform**:
  * The orchestrating DAG (which replaces the KSH wrapper) will define the sequential execution of this PySpark script. 
  * Because the downstream master database (`DW.DWH_ALL_TYPES_MASTER`) is not yet migrated, the PySpark script should write outputs to BigQuery tables, but also support exporting the final datasets to GCS in standard `.dat` file formats (simulating the legacy flat files) if immediate legacy consumption is required.

---

### 2. TARGET FILE PLAN
| Source File Path | Target File Path | Target Language | Purpose / Mapping |
| :--- | :--- | :--- | :--- |
| `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` | `TMD_processing/ALL_TYPES/mp/all_types_graph.py` | PySpark | Converts the Ab Initio graph into a modular PySpark job. It replicates the inline lookup building, splits the data stream into three transactional routes (Cancellations, Products, and Quotes/Contracts), performs date-filtering for weekly roll-overs, and writes standard and weekly results to separate targets. |

---

### 3. ENVIRONMENT-SPECIFIC VALUES

#### GLOBAL (Environment-Wide)
These constants identify the target cloud infrastructure and must be resolved at runtime using standard Python/Spark environment-variable lookups:
* **`GCP_PROJECT`**: The target Google Cloud Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")`.
* **`GCS_BUCKET`**: The target Cloud Storage bucket mapping legacy file system directories (e.g., `CCR_AI_DAT_FILE_DIR`, `TCN_DS_SERIAL_LOOKUP`). Sourced via `os.environ.get("GCS_BUCKET")`.
* **`BQ_DATASET`**: The default target BigQuery dataset where the team lookup source tables and processed output tables reside. Sourced via `os.environ.get("BQ_DATASET")`.
* **`DATAPROC_REGION`**: The GCP region where the Dataproc Serverless batch job is executed. Sourced via `os.environ.get("DATAPROC_REGION")`.

#### JOB-SPECIFIC
These values are specific to the functional scope of this script and must be supplied as job-level configurations or script arguments:
* **`CCR_AI_CUBE_CODE`**: Inherited legacy metadata string, value `"SPOS_FACTS"`. Defined as a local constant.
* **`SOURCE_TABLE_TEAMSICHTBARKEIT`**: Target BigQuery table replacing `CCR$TA_F_TEAMSICHTBARKEIT`. Hardcoded as `"ccr_ta_f_teamsichtbarkeit"`.
* **`SOURCE_TABLE_SDM_TEAM`**: Target BigQuery table replacing `ccr$ta_s_sdm_team`. Hardcoded as `"ccr_ta_s_sdm_team"`.
* **`SOURCE_TABLE_SDM_ABTEILUNG`**: Target BigQuery table replacing `ccr$ta_s_sdm_abteilung`. Hardcoded as `"ccr_ta_s_sdm_abteilung"`.
* **`SOURCE_MEASURES_TABLE`**: BigQuery table (or GCS path) replacing the primary flat-file input `x_tos_measures.dat`.

---

### 4. RISKS & MANUAL ACTIONS
* **Downstream Path Syncing**: The legacy graph writes out structured `.dat` files to local directories specified by `$CCR_AI_DAT_FILE_DIR`. If the downstream `DW.DWH_ALL_TYPES_MASTER` database load expects files in the legacy filesystem, Cloud Storage FUSE mounts must be configured, or the PySpark script must execute an explicit file-export step from BigQuery to a GCS bucket path mapped to that directory structure.
* **Lookup Generation Dependency**: The lookup table `lkp_team_virt_ccos` is generated dynamically using an Oracle query during graph execution. In the PySpark implementation, this lookup must be generated as a DataFrame beforehand and broadcast-joined to the streaming measure data to avoid performance degradation.
* **Legacy DB Connectivity**: The query for determining team virtuality accesses Oracle-specific tables (`CCR$TA_F_TEAMSICHTBARKEIT`, `ccr$ta_s_sdm_team`, `ccr$ta_s_sdm_abteilung`). A prerequisite task must replicate these tables to BigQuery, or the Dataproc Serverless job must be configured with a JDBC connector and network routing to access the legacy Oracle DB securely.

---

### 5. FILE DISPOSITION

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` | `TMD_processing/ALL_TYPES/mp/all_types_graph.py` | Primary conversion from Ab Initio GDE graph to a PySpark pipeline running on Dataproc Serverless. |