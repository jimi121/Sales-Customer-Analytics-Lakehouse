{% snapshot snapshot_crm_cust_info %}
{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='cst_id',
  updated_at='ingestion_timestamp',
) }}
SELECT * FROM {{ ref('silver_crm_cust_info') }}
{% endsnapshot %}