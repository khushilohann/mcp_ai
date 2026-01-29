# Example Queries for Testing

This document contains example queries you can use to test the unified search functionality across SQLite database, REST API, and local files.

## Basic User Queries

### Single Field Searches
- `show user with name user21`
- `find user with email user21@example.com`
- `user with id 36`
- `users in region EU`
- `users who signed up on 2025-01-22`

### Multiple Field Searches (AND)
- `user with name user21 and region EU`
- `email user21@example.com and region EU`
- `region NA and signup_date 2025-01-05`
- `name user5 and email user5@example.com`

### OR Operations
- `user with name user21 or name user22`
- `region EU or region NA`
- `email user21@example.com or email user22@example.com`
- `name user1 or name user2 or name user3`

### Complex Queries
- `users in region EU and signup_date 2025-01-06`
- `name user21 and (region EU or region NA)`
- `email user21@example.com and region EU and signup_date 2025-01-22`

## Date Range Queries

- `users signed up in January 2025`
- `signup_date 2025-01-22`
- `users who signed up last month`
- `signup_date between 2025-01-01 and 2025-01-31`

## Region-Based Queries

- `all users in region EU`
- `users in region NA or APAC`
- `region EU and signup_date 2025-01-06`
- `show me users from LATAM region`

## API-Specific Queries

- `show me data from api path /users`
- `get user details from api path /users/20`
- `api path /users with name ApiUser1`

## Cross-Source Queries (Searches All Sources)

The system automatically searches across:
- **SQLite Database** (users table with User1-User200)
- **REST API** (mock API with /users endpoint, User1-User150 + ApiUser1-ApiUser60)
- **Local Files** (users.csv and users.xlsx)

Examples:
- `show user with name user21` - Will search all three sources
- `email user21@example.com` - Will find user in database and CSV
- `region EU` - Will return users from all sources matching EU region

## Testing Tips

1. **Test Single Source**: Use specific queries like "show user with name user21" to see results from all sources
2. **Test AND Logic**: Try "name user21 and region EU" to see intersection filtering
3. **Test OR Logic**: Try "region EU or region NA" to see union results
4. **Test Date Filters**: Use "signup_date 2025-01-22" to filter by specific dates
5. **Test Cross-Source**: Queries automatically search DB + API + Files and merge results

## Expected Results

- Results are displayed in a **tabular format** (not raw JSON)
- Each result includes a `source` column indicating where it came from (sql/api/file)
- Duplicate results (same user from multiple sources) are deduplicated
- Results are sorted and formatted for easy reading

## Notes

- The unified search parser understands natural language queries
- Field names: `id`, `name`, `email`, `region`, `signup_date`
- Operators: `and`, `or` (case-insensitive)
- Date format: `YYYY-MM-DD` (e.g., `2025-01-22`)