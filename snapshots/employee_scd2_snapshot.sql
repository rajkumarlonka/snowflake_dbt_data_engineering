{% snapshot employee_scd2_snapshot %}

    {{
        config(
            target_schema='CURATED',
            unique_key='id',        
            strategy='check',       
            check_cols=['name', 'city'], 
            updated_at='last_updated' 
        )
    }}

    
    SELECT 
        id,
        name,
        city,
        current_timestamp() AS last_updated
    FROM {{ source('RAW', 'EMPLOYEE_SRC_SCD2') }} 

{% endsnapshot %}
