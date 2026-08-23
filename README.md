# TrogonEventStore Rust Client
[![Build Status][ci-badge]][ci-url]

[ci-badge]: https://github.com/TrogonStack/TrogonEventStore-Client-Rust/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/TrogonStack/TrogonEventStore-Client-Rust/actions

[Documentation](docs)

Community-maintained Rust gRPC client for TrogonEventStore.

TrogonEventStore is an event-native database where business events are immutably stored and streamed.

## Server compatibility
This client is compatible with version `20.6.1` upwards and works on Linux, MacOS and Windows.


Server setup instructions are available in the [TrogonEventStore repository].

## Installation

```toml
[dependencies]
trogon-eventstore = { git = "https://github.com/TrogonStack/TrogonEventStore-Client-Rust", tag = "trogon-eventstore@v0.1.0" }
```

Cargo registry publishing is not currently configured. Use a tagged GitHub release as the dependency source.

## Example

```rust
use trogon_eventstore::{Client, EventData};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct Foo {
    is_rust_a_nice_language: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Creates a client settings for a single node configuration.
    let settings = "esdb://admin:changeit@localhost:2113".parse()?;
    let client = Client::new(settings)?;

    let payload = Foo {
        is_rust_a_nice_language: true,
    };

    // It is not mandatory to use JSON as a data format, but TrogonEventStore
    // provides great additional value if you do so.
    let evt = EventData::json("language-poll", &payload)?;

    client
        .append_to_stream("language-stream", &Default::default(), evt)
        .await?;

    let mut stream = client
        .read_stream("language-stream", &Default::default())
        .await?;

    while let Some(event) = stream.next().await? {
        let event = event.get_original_event()
          .as_json::<Foo>()?;

        // Do something productive with the result.
        println!("{:?}", event);
    }

    Ok(())
}
```

## Support

Use [GitHub Discussions] for support questions.

## Documentation

Project documentation is maintained in this repository.

Bear in mind that this client is not yet properly documented. We are working hard on a new version of the documentation.

## License

TrogonEventStore Rust Client is licensed under the Apache License 2.0. It is derived from software originally licensed under the MIT License. The complete inherited MIT notice is preserved in [LICENSES/MIT.txt](LICENSES/MIT.txt).

## Communities

- [GitHub Discussions]

[GitHub Discussions]: https://github.com/TrogonStack/TrogonEventStore-Client-Rust/discussions
[TrogonEventStore repository]: https://github.com/TrogonStack/TrogonEventStore
