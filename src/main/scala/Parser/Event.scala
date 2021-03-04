package Parser

case class Event(
                    id: String,
                    org: String,
                    actor: Actor,
                    publicField: Boolean,
                    `type`: String,
                    repo: String,
                    payload: Payload,
                    created_at: java.sql.Timestamp
                  )
