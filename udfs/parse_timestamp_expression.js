-- BigQuery JavaScript UDF for parsing and applying custom timestamp expressions.
-- Translates timestamp logic from vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Handles expressions like '1d', '2y-3m', '5hT' (truncates to hour).
-- This UDF provides more flexibility for complex regex parsing than pure SQL.

CREATE OR REPLACE FUNCTION project.dataset.parse_timestamp_expression(
    expression STRING,
    sysdate_str STRING -- 'YYYY-MM-DD HH:MI:SS' or similar
)
RETURNS TIMESTAMP
LANGUAGE js
AS """
    function parseDuration(durationStr) {
        const regex = /(\\d+)\\s*([ymdwhis])/g;
        let match;
        const durations = {
            'y': 0, 'm': 0, 'd': 0, 'w': 0, 'h': 0, 'i': 0, 's': 0
        };
        while ((match = regex.exec(durationStr)) !== null) {
            durations[match[2]] += parseInt(match[1]);
        }
        return durations;
    }

    function applyDuration(date, durations, sign) {
        let newDate = new Date(date);
        if (durations.y) newDate.setFullYear(newDate.getFullYear() + sign * durations.y);
        if (durations.m) newDate.setMonth(newDate.getMonth() + sign * durations.m);
        if (durations.d) newDate.setDate(newDate.getDate() + sign * durations.d);
        if (durations.w) newDate.setDate(newDate.getDate() + sign * durations.w * 7); // Weeks to days
        if (durations.h) newDate.setHours(newDate.getHours() + sign * durations.h);
        if (durations.i) newDate.setMinutes(newDate.getMinutes() + sign * durations.i);
        if (durations.s) newDate.setSeconds(newDate.getSeconds() + sign * durations.s);
        return newDate;
    }

    const trimmedExpr = expression.trim();
    const parts = trimmedExpr.match(/^([0-9]+\\s*[ymdwhis][0-9]*[t]?)?(\\s*[+-]\\s*[0-9]+\\s*[ymdwhis][0-9]*[t]?)*/);

    if (!parts) {
        return null; // Invalid expression format
    }

    let currentDate = new Date(sysdate_str);
    if (isNaN(currentDate.getTime())) {
        currentDate = new Date(); // Fallback to current time if sysdate_str is invalid
    }

    const tokens = trimmedExpr.match(/([+-]?[0-9]+\\s*[ymdwhis])([t]?)?/g);

    if (!tokens) {
        return null; // No duration tokens found
    }

    let finalDate = currentDate;

    for (const token of tokens) {
        const match = token.match(/^([+-]?)(\\d+)\\s*([ymdwhis])$/);
        if (match) {
            const sign = (match[1] === '-') ? -1 : 1;
            const value = parseInt(match[2]);
            const unit = match[3];

            const durations = {};
            durations[unit] = value;
            finalDate = applyDuration(finalDate, durations, sign);
        }
    }

    // Handle truncation 't' - this logic was not fully specified but implied in original ksh date handling
    if (trimmedExpr.endsWith('t')) {
        const lastUnitMatch = trimmedExpr.match(/([ymdwhis])(?=[0-9]*[t]?$)/);
        if (lastUnitMatch) {
            const unit = lastUnitMatch[1];
            switch (unit) {
                case 'y': finalDate.setMonth(0); finalDate.setDate(1); finalDate.setHours(0,0,0,0); break;
                case 'm': finalDate.setDate(1); finalDate.setHours(0,0,0,0); break;
                case 'd': finalDate.setHours(0,0,0,0); break;
                case 'h': finalDate.setMinutes(0,0,0); break;
                case 'i': finalDate.setSeconds(0,0); break;
                case 's': finalDate.setMilliseconds(0); break;
            }
        }
    }

    // Convert to BigQuery TIMESTAMP format (UTC)
    return finalDate.toISOString().replace('Z', '+00:00');
""";