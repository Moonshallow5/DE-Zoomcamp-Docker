{% macro get_vendor_names(vendor_id) %}
case {{ vendor_id }}
    when 1 then 'Creative Mobile Technologies, LLC'
    when 2 then 'VeriFone Inc.'
    else 'Unknown'
end
{% endmacro %}