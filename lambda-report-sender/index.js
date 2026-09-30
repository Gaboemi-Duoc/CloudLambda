import { randomUUID } from "node:crypto";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const sns = new SNSClient({});
const TOPIC_ARN = process.env.SNS_TOPIC_ARN;

export const handler = async (event) => {
  try {
    const requestedBy = extractEmail(event);

    const request = {
      requestId: randomUUID(),
      requestedBy,
      requestedAt: new Date().toISOString(),
    };

    await sns.send(
      new PublishCommand({
        TopicArn: TOPIC_ARN,
        Subject: "Course report requested",
        Message: JSON.stringify(request),
        // --- Filter policy match ---
        MessageAttributes: {
          type: {
            DataType: "String",
            StringValue: "Course",
          },
        },
      })
    );

    console.log(
      `Published Course report request ${request.requestId} from ${requestedBy}`
    );

    return {
      statusCode: 202,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        status: "accepted",
        requestId: request.requestId,
      }),
    };
  } catch (err) {
    console.error("Failed to queue report", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        status: "error",
        message: "Report could not be queued",
      }),
    };
  }
};

function extractEmail(event) {
  const claims = event?.requestContext?.authorizer?.jwt?.claims;
  if (claims?.email) return String(claims.email);
  return "system";
}