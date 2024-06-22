# Kafka Grid Mix

This sample project uses Kafka to record messages about the cleanliness of the grid for power customers. The data comes from the EIA electricity fuel type
API, and is updated hourly. The data is then processed and sent to Kafka topics based on whether the utility generation is mostly clean (Solar, Hydro, Nuclear, Wind, etc.) or mostly dirty (Fossil fuels like Coal, Oil, and Gas).

A separate consumer then listens for these messages and performs further processing for display.

## Prerequisites

You'll need an EIA API key to run this project. You can get one by signing up at https://www.eia.gov/opendata/register.php.
Set it to the `EIA_API_KEY` environment variable.

```shell
export EIA_API_KEY=your_api_key
```

## Getting Started

1. Clone this repository:

    ```shell
    git clone https://github.com/dkarneman/kafka-grid-mix.git
    ```

2. Install and start Kafka. I used the Kafka docker image from https://hub.docker.com/r/apache/kafka#!

    ```shell
    docker pull apache/kafka:3.7.0
    docker run -p 9092:9092 apache/kafka:3.7.0
    ```

3. Install the requirements and run the project:

    ```shell
    pip install -r requirements.txt
    python producer.py  # This will start producing messages to the Kafka topics
    # In a separate terminal
    python consumer.py  # This will start consuming messages from the Kafka topics and print them to the console
    ```

You'll begin to see messages like this from the consumer:

```
The power was 51% clean for Duke Energy Progress East customers on June 22 at 12AM!
The power was 64% clean for Idaho Power Company customers on June 22 at 03AM!
The power was 85% clean for PacifiCorp West customers on June 22 at 03AM!
The power was 55% clean for Western Area Power Administration - Desert Southwest Region customers on June 22 at 03AM!
```

By decoupling the producer and consumer, you can easily add more consumers to perform different processing on the messages, such as displaying them on a
dashboard or texting them to a user to know when it's a good time to charge a vehicle or run a high-energy appliance.
