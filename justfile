# Start HyperDX + event generator
up:
    docker compose up -d --build

# Stop all services
down:
    docker compose down

# Tail event generator logs
logs:
    docker compose logs -f event-generator

# Stop the event generator (HyperDX keeps running)
stop:
    docker compose stop event-generator

# Start the event generator
start:
    docker compose start event-generator

# Restart the event generator
restart:
    docker compose restart event-generator
