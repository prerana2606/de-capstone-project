with users as (
    select distinct user_id
    from {{ ref('stg_posts') }}
)
select * from users
