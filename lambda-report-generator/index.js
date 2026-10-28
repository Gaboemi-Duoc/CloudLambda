const { Pool } = require('pg');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const pool = new Pool({ 
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

exports.handler = async (event) => {
    if (event.Records && event.Records.length > 0) {
        for (const record of event.Records) {
            try {
                const snsEnvelope = JSON.parse(record.body); 
                const payload = typeof snsEnvelope.Message === 'string' ? JSON.parse(snsEnvelope.Message) : snsEnvelope.Message;
                await handleAction(payload.action, payload.data);
            } catch (error) {
                console.error("SQS Processing Error:", error);
                throw error;
            }
        }
        return { statusCode: 200, body: 'SQS Records processed' };
    }

    const body = typeof event.body === 'string' ? JSON.parse(event.body) : event;
    try {
        return await handleAction(body.action, body);
    } catch (error) {
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};

async function handleAction(action, data) {
    if (action === 'GENERATE_REPORT') {
        const query = `
            SELECT 
                c.id AS course_id, 
                c.name AS course_name, 
                c.teacher_id,
                COUNT(DISTINCT e.id) AS student_count,
                COUNT(DISTINCT t.id) AS task_count
            FROM courses c
            LEFT JOIN enrollments e ON c.id = e.course_id
            LEFT JOIN tasks t ON c.id = e.course_id
            GROUP BY c.id, c.name, c.teacher_id
            ORDER BY c.id;
        `;
        const { rows } = await pool.query(query);

        const savedReport = await pool.query(
            'INSERT INTO reports(report_data, created_at) VALUES($1, NOW()) RETURNING id, created_at',
            [JSON.stringify(rows)]
        );

        let notificationText = `📊 **Course Report (ID: #${savedReport.rows[0].id})**\n\n`;
        rows.forEach(c => {
            notificationText += `📚 **${c.course_name}**\n`;
            notificationText += `   👨‍🎓 Students: ${c.student_count}\n`;
            notificationText += `   📝 Tasks: ${c.task_count}\n\n`;
        });

        if (process.env.SNS_TOPIC_ARN) {
            await snsClient.send(new PublishCommand({
                TopicArn: process.env.SNS_TOPIC_ARN,
                Message: JSON.stringify({ 
                    type: 'REPORT_GENERATED', 
                    message: notificationText,
                    data: { 
                        reportId: savedReport.rows[0].id,
                        requestedBy: data.requestedBy, // <-- Pass the email
                        reportData: rows               // <-- Pass the raw data for the email
                    }
                }),
                MessageAttributes: { 
                    "type": {
                        DataType: "String",
                        StringValue: "REPORT_GENERATED"
                    }
                }
            }));
        }

        return { statusCode: 201, body: JSON.stringify({ message: "Report generated", reportId: savedReport.rows[0].id }) };
    }
    throw new Error(`Unknown action: ${action}`);
}