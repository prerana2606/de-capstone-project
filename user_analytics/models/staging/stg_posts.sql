with posts as (
    select
        raw_json:"userId"::int as user_id,
        raw_json:"id"::int as post_id,
        raw_json:"title"::string as title,
        raw_json:"body"::string as body
    from {{ source('raw_data', 'posts') }}
)
select * from posts
