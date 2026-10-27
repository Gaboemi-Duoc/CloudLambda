const { Pool } = require('pg');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const pool = new Pool({ 
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

const publishEvent = async (type, message, data) => {
    if (!process.env.SNS_TOPIC_ARN) return;
    await snsClient.send(new PublishCommand({
        TopicArn: process.env.SNS_TOPIC_ARN,
        Message: JSON.stringify({ type, message, data }),
        MessageAttributes: { // <-- Added for SNS Filter Policy
            "type": {
                DataType: "String",
                StringValue: type
            }
        }
    }));
};

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
    switch (action) {
        case 'TEACHER_CREATE_TASK':
            const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [data.courseId]);
            if (course.rows[0].teacher_id !== data.teacherId) throw new Error("Not the owner");
            
            const newTask = await pool.query(
                'INSERT INTO tasks(course_id, title, description, due_date, created_at) VALUES($1, $2, $3, $4, NOW()) RETURNING *',
                [data.courseId, data.data.title, data.data.description, data.data.dueDate]
            );
            await publishEvent('TASK_CREATED', `New task created: ${newTask.rows[0].title}`, newTask.rows[0]);
            return { statusCode: 201, body: JSON.stringify(newTask.rows[0]) };

        case 'TEACHER_GET_TASKS':
            const teacherCheck = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [data.courseId]);
            if (teacherCheck.rows[0].teacher_id !== data.teacherId) return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
            const tasks = await pool.query('SELECT * FROM tasks WHERE course_id = $1', [data.courseId]);
            return { statusCode: 200, body: JSON.stringify(tasks.rows) };

        case 'STUDENT_GET_COURSE_TASKS':
            const enrolled = await pool.query('SELECT * FROM enrollments WHERE course_id = $1 AND student_id = $2', [data.courseId, data.studentId]);
            if (enrolled.rows.length === 0) return { statusCode: 403, body: JSON.stringify({ error: "You are not enrolled in this course" }) };
            const courseTasks = await pool.query('SELECT * FROM tasks WHERE course_id = $1', [data.courseId]);
            return { statusCode: 200, body: JSON.stringify(courseTasks.rows) };

        case 'STUDENT_GET_TASKS':
            const allMyTasks = await pool.query(`
                SELECT t.* FROM tasks t 
                JOIN enrollments e ON t.course_id = e.course_id 
                WHERE e.student_id = $1
            `, [data.studentId]);
            return { statusCode: 200, body: JSON.stringify(allMyTasks.rows) };

        default:
            throw new Error(`Unknown action: ${action}`);
    }
}