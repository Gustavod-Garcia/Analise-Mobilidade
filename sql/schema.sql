-- Schema para dados GTFS da SPTrans

-- Tabela: Agency (Operadora)
CREATE TABLE IF NOT EXISTS agency (
    agency_id VARCHAR(50) PRIMARY KEY,
    agency_name VARCHAR(255),
    agency_url VARCHAR(255),
    agency_timezone VARCHAR(50)
);

-- Tabela: Routes (Linhas)
CREATE TABLE IF NOT EXISTS routes (
    route_id VARCHAR(50) PRIMARY KEY,
    agency_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    route_type INTEGER,
    route_color VARCHAR(10),
    route_text_color VARCHAR(10),
    FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);

-- Tabela: Stops (Paradas)
CREATE TABLE IF NOT EXISTS stops (
    stop_id VARCHAR(50) PRIMARY KEY,
    stop_name VARCHAR(255),
    stop_desc TEXT,
    stop_lat DECIMAL(10, 8),
    stop_lon DECIMAL(11, 8)
);

-- Tabela: Trips (Viagens)
CREATE TABLE IF NOT EXISTS trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    route_id VARCHAR(50),
    service_id VARCHAR(50),
    trip_headsign VARCHAR(255),
    direction_id INTEGER,
    shape_id VARCHAR(50),
    FOREIGN KEY (route_id) REFERENCES routes(route_id)
);

-- Tabela: Stop Times (Horários nas Paradas)
CREATE TABLE IF NOT EXISTS stop_times (
    id SERIAL PRIMARY KEY,
    trip_id VARCHAR(50),
    arrival_time VARCHAR(20),
    departure_time VARCHAR(20),
    stop_id VARCHAR(50),
    stop_sequence INTEGER,
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

-- Tabela: Calendar (Calendário de Operação)
CREATE TABLE IF NOT EXISTS calendar (
    service_id VARCHAR(50) PRIMARY KEY,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date DATE,
    end_date DATE
);

-- Tabela: Shapes (Traçado das Rotas)
CREATE TABLE IF NOT EXISTS shapes (
    id SERIAL PRIMARY KEY,
    shape_id VARCHAR(50),
    shape_pt_lat DECIMAL(10, 8),
    shape_pt_lon DECIMAL(11, 8),
    shape_pt_sequence INTEGER,
    shape_dist_traveled DECIMAL(10, 2)
);

-- Índices para melhorar performance
CREATE INDEX idx_routes_agency ON routes(agency_id);
CREATE INDEX idx_trips_route ON trips(route_id);
CREATE INDEX idx_stop_times_trip ON stop_times(trip_id);
CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX idx_shapes_shape_id ON shapes(shape_id);
CREATE INDEX idx_stops_location ON stops(stop_lat, stop_lon);
