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
            case 'ADMIN_ENROLL_STUDENT':
            case 'TEACHER_ENROLL_STUDENT':
            case 'STUDENT_ENROLL':
                // 1. Ownership check for teachers
                if (action === 'TEACHER_ENROLL_STUDENT') {
                    const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [courseId]);
                    if (course.rows.length === 0) return { statusCode: 404, body: JSON.stringify({ error: "Course not found" }) };
                    if (course.rows[0].teacher_id !== teacherId) return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
                }

                // 2. Check if already enrolled
                const targetStudent = action === 'STUDENT_ENROLL' ? studentId : data.studentId;
                const existing = await pool.query('SELECT * FROM enrollments WHERE course_id = $1 AND student_id = $2', [courseId, targetStudent]);
                if (existing.rows.length > 0) {
                    return { statusCode: 200, body: JSON.stringify(existing.rows[0]) };
                }

                // 3. Create enrollment
                const newEnrollment = await pool.query(
                    'INSERT INTO enrollments(course_id, student_id, created_at) VALUES($1, $2, NOW()) RETURNING *',
                    [courseId, targetStudent]
                );
                await publishEvent('STUDENT_ENROLLED', `Student ${targetStudent} enrolled in course ${courseId}`, newEnrollment.rows[0]);
                return { statusCode: 201, body: JSON.stringify(newEnrollment.rows[0]) };

            case 'ADMIN_GET_STUDENTS':
            case 'TEACHER_GET_STUDENTS':
                if (action === 'TEACHER_GET_STUDENTS') {
                    const course = await pool.query('SELECT teacher_id FROM courses WHERE id = $1', [courseId]);
                    if (course.rows[0].teacher_id !== teacherId) return { statusCode: 403, body: JSON.stringify({ error: "Not the owner" }) };
                }
                const students = await pool.query(`
                    SELECT e.id, e.course_id as "courseId", c.name as "courseName", e.student_id as "studentId", e.created_at as "createdAt"
                    FROM enrollments e JOIN courses c ON e.course_id = c.id WHERE e.course_id = $1
                `, [courseId]);
                return { statusCode: 200, body: JSON.stringify(students.rows) };

            case 'STUDENT_GET_COURSES':
                const myCourses = await pool.query(`
                    SELECT e.id, e.course_id as "courseId", c.name as "courseName", c.description, e.student_id as "studentId", e.created_at as "createdAt"
                    FROM enrollments e JOIN courses c ON e.course_id = c.id WHERE e.student_id = $1
                `, [studentId]);
                return { statusCode: 200, body: JSON.stringify(myCourses.rows) };

            default:
                return { statusCode: 400, body: JSON.stringify({ error: "Unknown action" }) };
        }
    } catch (error) {
        console.error(error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};