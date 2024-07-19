# Examples

The easiest way to run examples on Linux is to use the provided `docker-compose.yaml` file. To run the example, you need to have docker and docker-compose installed on your machine.

1. Build examples

    ```bash
    cargo build --examples
    ```

2. Launch the docker-compose file

    ```bash
    docker-compose up
    ```

3. See the logs or inspect MongoDB data via MongoDB Compass
