const { Pool } = require('pg');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const pool = new Pool({ 
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

const publishEvent = async (type, message, data) => {
    if (!process.env.SNS_TOPIC_ARN) return;
    await snsClient.send(new PublishCommand({
        TopicArn: process.env.SNS_TOPIC_ARN,
        Message: JSON.stringify({ type, message, data }),
    }));
};

exports.handler = async (event) => {
    const body = typeof event.body === 'string' ? JSON.parse(event.body) : event;
    const { action, courseId, teacherId, studentId, data } = body;

    try {
        switch (action) {
            case 'TEACHER_CREATE_TASK':
                // Ownership Check
                const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [courseId]);
                if (course.rows[0].teacher_id !== teacherId) {
                    return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
                }
                
                const newTask = await pool.query(
                    'INSERT INTO tasks(course_id, title, description, due_date, created_at) VALUES($1, $2, $3, $4, NOW()) RETURNING *',
                    [courseId, data.title, data.description, data.dueDate]
                );
                await publishEvent('TASK_CREATED', `New task created: ${newTask.rows[0].title}`, newTask.rows[0]);
                return { statusCode: 201, body: JSON.stringify(newTask.rows[0]) };

            case 'TEACHER_GET_TASKS':
                const teacherCheck = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [courseId]);
                if (teacherCheck.rows[0].teacher_id !== teacherId) {
                    return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
                }
                const tasks = await pool.query('SELECT * FROM tasks WHERE course_id = $1', [courseId]);
                return { statusCode: 200, body: JSON.stringify(tasks.rows) };

            case 'STUDENT_GET_COURSE_TASKS':
                // Verify enrollment
                const enrolled = await pool.query('SELECT * FROM enrollments WHERE course_id = $1 AND student_id = $2', [courseId, studentId]);
                if (enrolled.rows.length === 0) {
                    return { statusCode: 403, body: JSON.stringify({ error: "You are not enrolled in this course" }) };
                }
                const courseTasks = await pool.query('SELECT * FROM tasks WHERE course_id = $1', [courseId]);
                return { statusCode: 200, body: JSON.stringify(courseTasks.rows) };

            case 'STUDENT_GET_TASKS':
                const allMyTasks = await pool.query(`
                    SELECT t.* FROM tasks t 
                    JOIN enrollments e ON t.course_id = e.course_id 
                    WHERE e.student_id = $1
                `, [studentId]);
                return { statusCode: 200, body: JSON.stringify(allMyTasks.rows) };

            default:
                return { statusCode: 400, body: JSON.stringify({ error: "Unknown action" }) };
        }
    } catch (error) {
        console.error(error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};