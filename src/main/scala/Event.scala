case class Event(
                  actor: Actor,
                  create_at: String,
                  id: BigInt,
                  org: String,
                  payload: Payload,
                  `public`: Boolean,
                  repo: String,
                  `type`: String
                )