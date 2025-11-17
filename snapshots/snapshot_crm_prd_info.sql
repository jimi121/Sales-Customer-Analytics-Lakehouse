{% snapshot snapshot_crm_prd_info %}
{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='prd_id',
  updated_at='ingestion_timestamp',
) }}
SELECT * FROM {{ ref('silver_crm_prd_info') }}
{% endsnapshot %}