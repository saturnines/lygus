# Build stage
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)

# Runtime stage
FROM ubuntu:24.04

WORKDIR /app

COPY --from=builder /build/build/lygus-server /app/lygus-server

RUN mkdir -p /data

EXPOSE 8080 5000 5001

ENTRYPOINT ["stdbuf", "-oL", "-eL", "/app/lygus-server"]