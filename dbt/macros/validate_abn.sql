{% macro validate_abn(abn_field) %}
/*
    Macro to validate Australian Business Numbers (ABNs).
    Returns true if the ABN passes format and checksum validation.
*/
    case
        when {{ abn_field }} is null then null
        when length({{ abn_field }}) != 11 then false
        when {{ abn_field }} !~ '^[0-9]{11}$' then false
        else
            -- ABN checksum validation
            (
                (
                    (cast(substring({{ abn_field }}, 1, 1) as integer) - 1) * 10 +
                    cast(substring({{ abn_field }}, 2, 1) as integer) * 1 +
                    cast(substring({{ abn_field }}, 3, 1) as integer) * 3 +
                    cast(substring({{ abn_field }}, 4, 1) as integer) * 5 +
                    cast(substring({{ abn_field }}, 5, 1) as integer) * 7 +
                    cast(substring({{ abn_field }}, 6, 1) as integer) * 9 +
                    cast(substring({{ abn_field }}, 7, 1) as integer) * 11 +
                    cast(substring({{ abn_field }}, 8, 1) as integer) * 13 +
                    cast(substring({{ abn_field }}, 9, 1) as integer) * 15 +
                    cast(substring({{ abn_field }}, 10, 1) as integer) * 17 +
                    cast(substring({{ abn_field }}, 11, 1) as integer) * 19
                ) % 89
            ) = 0
    end
{% endmacro %}