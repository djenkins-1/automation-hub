# Self-hosted event routing service
This service is designed to receive events and notifications from any sources to simplify the workflows in your life. An HTTP sink is preconfigured but any new systems can be easily added. Once an event is received, a job is added to the queue according to the rules configured and will be picked up by a worker thread when available.

Future plans for this project include integrations with more services such as smart-home appliances, receiving emails, Slack notifications, and more!

Instructions will be added once a stable release is available.
