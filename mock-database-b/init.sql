CREATE TABLE IF NOT EXISTS customer_regions (
    id INT NOT NULL,
    region VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO customer_regions (id, region) VALUES
    (1, 'EMEA'),
    (2, 'APAC'),
    (3, 'AMER'),
    (4, 'LATAM');
