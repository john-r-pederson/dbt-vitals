-- stg_users: staging model for raw user data
select
    user_id,
    username,
    created_at
from raw.users
