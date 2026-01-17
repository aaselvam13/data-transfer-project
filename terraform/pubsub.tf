resource "google_pubsub_topic" "topic" {
  name = var.topic_name

}

resource "google_pubsub_subscription" "subscription" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.id

  retain_acked_messages = false
  
}