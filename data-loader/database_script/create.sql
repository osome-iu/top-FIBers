-- create table reports
create table reports(
	id SERIAL PRIMARY KEY,
	date DATE NOT NULL,
	name VARCHAR(255) NOT NULL,
	platform VARCHAR(20)
);

-- create table fib_indices
create table fib_indices(
	fib_index_id SERIAL PRIMARY KEY,
	user_id VARCHAR(100) NOT NULL,
	report_id INT NOT NULL,
	fib_index NUMERIC(5, 2) NOT NULL,
	total_reshares INT NOT NULL,
	username VARCHAR(255) NOT NULL,
	platform VARCHAR(20) NOT NULL,
	CONSTRAINT fk_fib_indices FOREIGN KEY(report_id) REFERENCES reports(id)
);

-- create table posts
create table posts(
	id SERIAL PRIMARY KEY,
	post_id VARCHAR(100) NOT NULL,
	user_id varchar(100) NOT NULL,
	platform VARCHAR(20) NOT NULL,
	timestamp VARCHAR(150) NOT NULL,
	url varchar(255) NOT NULL
);

-- create table reshares
create table reshares(
	post_id VARCHAR(100) NOT NULL,
	report_id INT NOT NULL,
	platform VARCHAR(20) NOT NULL,
	num_reshares INT NOT NULL,
	CONSTRAINT pk_reshares PRIMARY KEY(post_id,report_id,platform)
);

-- create table profile_links
create table profile_links(
	id SERIAL PRIMARY KEY,
	user_id VARCHAR(100) NOT NULL,
	platform VARCHAR(20),
	profile_image_url VARCHAR(255)
);

CREATE INDEX idx_reshares_post_id ON reshares (post_id);
CREATE INDEX idx_posts_user_id ON posts (user_id);
CREATE INDEX idx_posts_platform ON posts (platform);
CREATE INDEX idx_fib_indices_user_id ON fib_indices(user_id);
CREATE INDEX idx_reports_name ON reports(name);


--ACCESS DB
REVOKE CONNECT ON DATABASE topfibers FROM PUBLIC;
GRANT  CONNECT ON DATABASE topfibers  TO  topfibers;

--ACCESS SCHEMA
REVOKE ALL     ON SCHEMA public FROM PUBLIC;
GRANT  USAGE   ON SCHEMA public  TO  topfibers;

GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO topfibers;

--ACCESS TABLES
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC ;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO topfibers;
GRANT ALL                            ON ALL TABLES IN SCHEMA public TO topfibers ;
