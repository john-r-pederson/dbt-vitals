-- stg_users: staging model for raw user data
select
    id,
    email,
    created_at
from raw.users
