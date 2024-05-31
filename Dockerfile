
FROM rust:1.76

# NOTE(DD): The same as in wallets, we assume that the host's user and group ids are `1000`,
# otherwise, the generated binaries will be created under a user different from the host's.
# If your user's ids are different, refer to Wallet's readme for information on how to solve
# this issue.
ARG UNAME=app
ARG UID=1000
ARG GID=1000

RUN groupadd -g $GID -o $UNAME
RUN useradd --uid $UID --gid $GID --system --create-home --no-log-init app
USER app

RUN rustup component add rustfmt
RUN rustup component add clippy
RUN cargo install --locked cargo-deny

# DD: certain external tools require nightly features.
# Should not be used in production
RUN rustup toolchain install nightly

# DD: search for unused dependencies
RUN cargo install cargo-udeps --locked

# DD: analyze binary size
RUN cargo install cargo-bloat

RUN cargo install cargo-unused-features
