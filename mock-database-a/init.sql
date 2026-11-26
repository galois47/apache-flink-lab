CREATE TABLE IF NOT EXISTS membership_tiers (
    id INT NOT NULL,
    tier VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO membership_tiers (id, tier) VALUES
    (1, 'A'),
    (2, 'B'),
    (3, 'C'),
    (4, 'D');
