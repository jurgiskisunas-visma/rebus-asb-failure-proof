// These are integration tests against a single shared Service Bus emulator instance, and several
// of them use the same queue. Running them in parallel makes them drain each other's messages.
[assembly: DoNotParallelize]
