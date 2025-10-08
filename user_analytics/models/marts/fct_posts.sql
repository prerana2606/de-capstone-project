select
    p.post_id,
    p.user_id,
    p.title,
    coalesce(c.number_of_comments, 0) as number_of_comments
from {{ ref('stg_posts') }} p
left join {{ ref('int_comments_per_post') }} c
    on p.post_id = c.post_id