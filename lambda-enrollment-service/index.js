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
        case 'ADMIN_ENROLL_STUDENT':
        case 'TEACHER_ENROLL_STUDENT':
        case 'STUDENT_ENROLL':
            if (action === 'TEACHER_ENROLL_STUDENT') {
                const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [data.courseId]);
                if (course.rows.length === 0) throw new Error("Course not found");
                if (course.rows[0].teacher_id !== data.teacherId) throw new Error("Not the owner");
            }

            const targetStudent = action === 'STUDENT_ENROLL' ? data.studentId : data.studentId;
            const existing = await pool.query('SELECT * FROM enrollments WHERE course_id = $1 AND student_id = $2', [data.courseId, targetStudent]);
            if (existing.rows.length > 0) return { statusCode: 200, body: JSON.stringify(existing.rows[0]) };

            const newEnrollment = await pool.query(
                'INSERT INTO enrollments(course_id, student_id, created_at) VALUES($1, $2, NOW()) RETURNING *',
                [data.courseId, targetStudent]
            );
            await publishEvent('STUDENT_ENROLLED', `Student ${targetStudent} enrolled in course ${data.courseId}`, newEnrollment.rows[0]);
            return { statusCode: 201, body: JSON.stringify(newEnrollment.rows[0]) };

        case 'ADMIN_GET_STUDENTS':
        case 'TEACHER_GET_STUDENTS':
            if (action === 'TEACHER_GET_STUDENTS') {
                const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [data.courseId]);
                if (course.rows[0].teacher_id !== data.teacherId) return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
            }
            const students = await pool.query(`
                SELECT e.id, e.course_id, c.name AS course_name, e.student_id, e.created_at
                FROM enrollments e JOIN courses c ON e.course_id = c.id WHERE e.course_id = $1
            `, [data.courseId]);
            return { statusCode: 200, body: JSON.stringify(students.rows) };

        case 'STUDENT_GET_COURSES':
            const myCourses = await pool.query(`
                SELECT e.id, e.course_id, c.name AS course_name, c.description, e.student_id, e.created_at
                FROM enrollments e JOIN courses c ON e.course_id = c.id WHERE e.student_id = $1
            `, [data.studentId]);
            return { statusCode: 200, body: JSON.stringify(myCourses.rows) };

        default:
            throw new Error(`Unknown action: ${action}`);
    }
}