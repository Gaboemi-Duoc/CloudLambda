const { Pool } = require('pg');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
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
            case 'ADMIN_LIST_COURSES':
                const allCourses = await pool.query('SELECT * FROM courses ORDER BY created_at DESC');
                return { statusCode: 200, body: JSON.stringify(allCourses.rows) };

            case 'ADMIN_GET_COURSE':
                const course = await pool.query('SELECT * FROM courses WHERE id = $1', [courseId]);
                if (course.rows.length === 0) return { statusCode: 404, body: JSON.stringify({ error: "Course not found" }) };
                return { statusCode: 200, body: JSON.stringify(course.rows[0]) };

            case 'ADMIN_CREATE_COURSE':
                const newCourse = await pool.query(
                    'INSERT INTO courses(name, description, teacher_id, created_at) VALUES($1, $2, $3, NOW()) RETURNING *',
                    [data.name, data.description, data.teacherId]
                );
                await publishEvent('COURSE_CREATED', `New course created: ${newCourse.rows[0].name}`, newCourse.rows[0]);
                return { statusCode: 201, body: JSON.stringify(newCourse.rows[0]) };

            case 'ADMIN_ASSIGN_TEACHER':
                const updated = await pool.query(
                    'UPDATE courses SET teacher_id = $1 WHERE id = $2 RETURNING *',
                    [teacherId, courseId]
                );
                await publishEvent('TEACHER_ASSIGNED', `Teacher assigned to course ID: ${courseId}`, updated.rows[0]);
                return { statusCode: 200, body: JSON.stringify(updated.rows[0]) };

            case 'ADMIN_DELETE_COURSE':
                await pool.query('DELETE FROM courses WHERE id = $1', [courseId]);
                await publishEvent('COURSE_DELETED', `Course deleted ID: ${courseId}`, { courseId });
                return { statusCode: 204, body: '' };

            case 'TEACHER_GET_COURSES':
                const teacherCourses = await pool.query('SELECT * FROM courses WHERE teacher_id = $1', [teacherId]);
                return { statusCode: 200, body: JSON.stringify(teacherCourses.rows) };

            case 'STUDENT_GET_AVAILABLE':
                const available = await pool.query(
                    'SELECT * FROM courses WHERE id NOT IN (SELECT course_id FROM enrollments WHERE student_id = $1)',
                    [studentId]
                );
                return { statusCode: 200, body: JSON.stringify(available.rows) };

            default:
                return { statusCode: 400, body: JSON.stringify({ error: "Unknown action" }) };
        }
    } catch (error) {
        console.error(error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};