CREATE TABLE IF NOT EXISTS membership_tiers (
    id INT NOT NULL,
    tier VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO membership_tiers (id, tier) VALUES
    (1, 'GOLD'),
    (2, 'SILVER'),
    (3, 'BRONZE');
