CREATE TABLE contact(
	id INT,
	phone_number VARCHAR(30),
	email VARCHAR(100),
	linked_id INT,
	link_precedence VARCHAR(9) NOT NULL,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	deleted_at DATETIME,
	PRIMARY KEY (id),
	CHECK (link_precedence IN ("primary", "secondary"))
);