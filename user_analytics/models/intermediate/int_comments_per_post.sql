with comment_counts as (
    select
        post_id,
        count(*) as number_of_comments
    from {{ ref('stg_comments') }}
    group by post_id
)
select * from comment_counts
