const { Pool } = require('pg');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const pool = new Pool({ 
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

exports.handler = async (event) => {
    const body = typeof event.body === 'string' ? JSON.parse(event.body) : event;
    const { action } = body;

    try {
        if (action === 'GENERATE_REPORT') {
            // 1. Fetch Aggregated Data
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

            // 2. Save to Database
            const savedReport = await pool.query(
                'INSERT INTO reports(report_data, created_at) VALUES($1, NOW()) RETURNING id, created_at',
                [JSON.stringify(rows)]
            );

            // 3. Format the notification text
            let notificationText = `📊 **Course Report (ID: #${savedReport.rows[0].id})**\n\n`;
            rows.forEach(c => {
                notificationText += `📚 **${c.course_name}**\n`;
                notificationText += `   👨‍🎓 Students: ${c.student_count}\n`;
                notificationText += `   📝 Tasks: ${c.task_count}\n\n`;
            });

            // 4. Publish to SNS
            if (process.env.SNS_TOPIC_ARN) {
                await snsClient.send(new PublishCommand({
                    TopicArn: process.env.SNS_TOPIC_ARN,
                    Message: JSON.stringify({ 
                        type: 'REPORT_GENERATED', 
                        message: notificationText,
                        data: { reportId: savedReport.rows[0].id }
                    }),
                    MessageGroupId: "CourseUpdates"
                }));
            }

            return { 
                statusCode: 201, 
                body: JSON.stringify({ 
                    message: "Report generated successfully", 
                    reportId: savedReport.rows[0].id,
                    data: rows
                }) 
            };
        }

        return { statusCode: 400, body: JSON.stringify({ error: "Unknown action" }) };
    } catch (error) {
        console.error(error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};