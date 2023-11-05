CREATE TABLE wildberries_data (
    id SERIAL PRIMARY KEY,
    metadata JSONB,
    state JSONB,
    version JSONB,
    params JSONB,
    data JSONB
);


# third task
SELECT * FROM wildberries_data
ORDER BY (data->>'position')::int
LIMIT 5;


# fourth task
SELECT data->>'brand', COUNT(*) as count
FROM wildberries_data
GROUP BY data->>'brand';


# fifth
SELECT AVG((data->>'salePriceU')::numeric) as avg_price
FROM wildberries_data;