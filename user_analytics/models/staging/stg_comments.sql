with comments as (
    select
        raw_json:"postId"::int as post_id,
        raw_json:"id"::int as comment_id,
        raw_json:"name"::string as name,
        raw_json:"email"::string as email,
        raw_json:"body"::string as body
    from {{ source('raw_data', 'comments') }}
)
select * from comments
