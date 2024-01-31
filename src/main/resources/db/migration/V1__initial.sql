CREATE TABLE foo_entity
(
    id   UUID not null,
    handled_at timestamp with time zone,
    PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS event_publication
(
    id               UUID                     NOT NULL,
    listener_id      TEXT                     NOT NULL,
    event_type       TEXT                     NOT NULL,
    serialized_event TEXT                     NOT NULL,
    publication_date TIMESTAMP WITH TIME ZONE NOT NULL,
    completion_date  TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);