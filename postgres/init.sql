-- ============================================================
--  GoodAir – Init PostgreSQL dans Docker
--  Exécuté automatiquement au 1er démarrage du conteneur
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tables
CREATE TABLE IF NOT EXISTS ville (
    id          SERIAL PRIMARY KEY,
    nom         VARCHAR(100)  NOT NULL,
    pays        CHAR(2)       NOT NULL DEFAULT 'FR',
    latitude    NUMERIC(9,6)  NOT NULL,
    longitude   NUMERIC(9,6)  NOT NULL,
    timezone    VARCHAR(50)   NOT NULL DEFAULT 'Europe/Paris',
    slug_aqicn  VARCHAR(100)  NOT NULL UNIQUE,
    actif       BOOLEAN       NOT NULL DEFAULT TRUE,
    cree_le     TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mesure_air_aqicn (
    id                BIGSERIAL PRIMARY KEY,
    ville_id          INTEGER      NOT NULL REFERENCES ville(id) ON DELETE CASCADE,
    collecte_le       TIMESTAMP    NOT NULL,
    mesure_le         TIMESTAMP,
    aqi_global        SMALLINT     CHECK (aqi_global >= 0 AND aqi_global <= 999),
    pm25              NUMERIC(7,2),
    pm10              NUMERIC(7,2),
    no2               NUMERIC(7,2),
    o3                NUMERIC(7,2),
    co                NUMERIC(7,2),
    so2               NUMERIC(7,2),
    temperature       NUMERIC(5,2),
    humidite          NUMERIC(5,2),
    pression          NUMERIC(8,2),
    vent              NUMERIC(5,2),
    polluant_dominant VARCHAR(20),
    station_nom       VARCHAR(200),
    station_idx       INTEGER
);

CREATE TABLE IF NOT EXISTS mesure_meteo (
    id                    BIGSERIAL PRIMARY KEY,
    ville_id              INTEGER      NOT NULL REFERENCES ville(id) ON DELETE CASCADE,
    collecte_le           TIMESTAMP    NOT NULL,
    mesure_le             TIMESTAMP,
    temperature           NUMERIC(5,2),
    temperature_ressentie NUMERIC(5,2),
    temp_min              NUMERIC(5,2),
    temp_max              NUMERIC(5,2),
    pression_hpa          SMALLINT,
    pression_mer_hpa      SMALLINT,
    pression_sol_hpa      SMALLINT,
    humidite_pct          SMALLINT     CHECK (humidite_pct BETWEEN 0 AND 100),
    vitesse_vent          NUMERIC(6,2),
    direction_vent        SMALLINT     CHECK (direction_vent BETWEEN 0 AND 360),
    couverture_nuages     SMALLINT     CHECK (couverture_nuages BETWEEN 0 AND 100),
    visibilite_m          INTEGER,
    condition_code        SMALLINT,
    condition_libelle     VARCHAR(50),
    condition_desc        VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mesure_pollution_owm (
    id          BIGSERIAL PRIMARY KEY,
    ville_id    INTEGER      NOT NULL REFERENCES ville(id) ON DELETE CASCADE,
    collecte_le TIMESTAMP    NOT NULL,
    mesure_le   TIMESTAMP,
    aqi_owm     SMALLINT     CHECK (aqi_owm BETWEEN 1 AND 5),
    co          NUMERIC(8,2),
    no          NUMERIC(8,2),
    no2         NUMERIC(8,2),
    o3          NUMERIC(8,2),
    so2         NUMERIC(8,2),
    pm2_5       NUMERIC(8,2),
    pm10        NUMERIC(8,2),
    nh3         NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS prevision_air (
    id          BIGSERIAL PRIMARY KEY,
    ville_id    INTEGER      NOT NULL REFERENCES ville(id) ON DELETE CASCADE,
    collecte_le TIMESTAMP    NOT NULL,
    jour        DATE         NOT NULL,
    polluant    VARCHAR(10)  NOT NULL,
    valeur_avg  NUMERIC(7,2),
    valeur_min  NUMERIC(7,2),
    valeur_max  NUMERIC(7,2),
    CONSTRAINT ck_polluant CHECK (polluant IN ('pm25','pm10','o3','uvi'))
);

CREATE TABLE IF NOT EXISTS pipeline_log (
    id             BIGSERIAL PRIMARY KEY,
    execute_le     TIMESTAMP    NOT NULL DEFAULT NOW(),
    source         VARCHAR(30)  NOT NULL,
    statut         VARCHAR(10)  NOT NULL,
    nb_villes      SMALLINT,
    nb_mesures     INTEGER,
    duree_secondes NUMERIC(8,2),
    message_erreur TEXT,
    CONSTRAINT ck_source CHECK (source IN ('AQICN','OWM_WEATHER','OWM_POLLUTION')),
    CONSTRAINT ck_statut CHECK (statut IN ('SUCCESS','ERROR','PARTIAL'))
);

-- Index
CREATE INDEX IF NOT EXISTS idx_aqicn_ville_temps      ON mesure_air_aqicn     (ville_id, collecte_le DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_ville_temps       ON mesure_meteo         (ville_id, collecte_le DESC);
CREATE INDEX IF NOT EXISTS idx_owm_ville_temps         ON mesure_pollution_owm (ville_id, collecte_le DESC);
CREATE INDEX IF NOT EXISTS idx_prevision_ville_jour    ON prevision_air        (ville_id, jour);
CREATE INDEX IF NOT EXISTS idx_pipeline_temps          ON pipeline_log         (execute_le DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_aqicn_ville_heure   ON mesure_air_aqicn     (ville_id, date_trunc('hour', collecte_le));
CREATE UNIQUE INDEX IF NOT EXISTS uq_meteo_ville_heure   ON mesure_meteo         (ville_id, date_trunc('hour', collecte_le));
CREATE UNIQUE INDEX IF NOT EXISTS uq_owm_ville_heure     ON mesure_pollution_owm (ville_id, date_trunc('hour', collecte_le));
CREATE UNIQUE INDEX IF NOT EXISTS uq_prev_ville_jour_pol ON prevision_air        (ville_id, jour, polluant, date_trunc('hour', collecte_le));

-- Vue Looker Studio / Power BI
CREATE OR REPLACE VIEW v_mesures_completes AS
SELECT
    v.nom           AS ville,
    v.latitude,
    v.longitude,
    a.collecte_le,
    a.aqi_global,
    a.pm25          AS aqicn_pm25,
    a.pm10          AS aqicn_pm10,
    a.no2           AS aqicn_no2,
    a.o3            AS aqicn_o3,
    m.temperature,
    m.humidite_pct,
    m.pression_hpa,
    m.vitesse_vent,
    m.condition_libelle,
    p.pm2_5         AS owm_pm25,
    p.pm10          AS owm_pm10,
    p.no2           AS owm_no2,
    p.aqi_owm
FROM ville v
LEFT JOIN mesure_air_aqicn     a ON a.ville_id = v.id
LEFT JOIN mesure_meteo         m ON m.ville_id = v.id
    AND date_trunc('hour', m.collecte_le) = date_trunc('hour', a.collecte_le)
LEFT JOIN mesure_pollution_owm p ON p.ville_id = v.id
    AND date_trunc('hour', p.collecte_le) = date_trunc('hour', a.collecte_le);

-- Données initiales
INSERT INTO ville (nom, pays, latitude, longitude, slug_aqicn) VALUES
    ('Paris',      'FR', 48.856614,  2.352219, 'paris'),
    ('Lyon',       'FR', 45.748000,  4.846000, 'lyon'),
    ('Marseille',  'FR', 43.296000,  5.381000, 'marseille'),
    ('Toulouse',   'FR', 43.605000,  1.444000, 'toulouse'),
    ('Bordeaux',   'FR', 44.837000, -0.580000, 'bordeaux'),
    ('Lille',      'FR', 50.629000,  3.057000, 'lille'),
    ('Strasbourg', 'FR', 48.574000,  7.752000, 'strasbourg'),
    ('Nice',       'FR', 43.710000,  7.262000, 'nice'),
    ('Nantes',     'FR', 47.218000, -1.553000, 'nantes'),
    ('Grenoble',   'FR', 45.188000,  5.724000, 'grenoble')
ON CONFLICT (slug_aqicn) DO NOTHING;