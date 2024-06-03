CREATE TABLE contact(
	id SERIAL,
	phone_number VARCHAR(30),
	email VARCHAR(100),
	linked_id INT,
	link_precedence VARCHAR(9) NOT NULL,
	created_at TIMESTAMP NOT NULL,
	updated_at TIMESTAMP NOT NULL,
	deleted_at TIMESTAMP,
	PRIMARY KEY (id),
	CHECK (link_precedence IN ('primary', 'secondary')),
	UNIQUE(email, phone_number)
);