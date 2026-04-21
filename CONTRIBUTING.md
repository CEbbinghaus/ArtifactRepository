# Contributing

## Contributing Guidelines

Thank you for considering contributing to the ARX project! We welcome any and all contributions, no matter how big or small. Whether you're fixing a typo or refactoring the entire backend, your contributions are valuable to us.

To ensure a positive and inclusive environment, we kindly ask all contributors to adhere to the following guidelines:

### Code of Conduct

Please review and abide by our [Code of Conduct](./CODE_OF_CONDUCT.md) in all discussions and interactions related to ARX, both within and outside of GitHub. We strive to maintain a safe and respectful space for everyone involved.

## Getting Started


## Documentation

If you would like to contribute to the ARX documentation, please ensure that your changes follow the guidelines outlined in the [docs/README.md](docs/README.md).

## Project Layout

### /common
This is the core implementation of ARX, All of the primitives are defined here as well as a lot of helpers, This will likely be published as a library eventually for programatic use by other applications.

### /client
The client is the CLI used to generate archives and manage commiting / restoring indexes to stores. It utilizes the [clap](https://github.com/clap-rs/clap) commandline parser library, and deals primarily with the filesystem

### /server
This is the HTTP server implementation built on the [axum](https://github.com/tokio-rs/axum) framework utilizing Tokio Async for high concurrent throughput. It is a lighweight Wrapper around the common Store implementation to provide the ability to read and write archives

### /docs
This directory contains all of the ARX documentation. As well as RFCs defining the ARX systems behavior and data layouts