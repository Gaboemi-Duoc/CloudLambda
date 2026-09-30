import pg from "pg";

const { Pool } = pg;

let pool;

function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 2,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    });
  }
  return pool;
}

export const handler = async (event) => {
  for (const record of event.Records) {
    await processMessage(record.body);
  }
};

async function processMessage(body) {
  const envelope = JSON.parse(body);
  const payload = JSON.parse(envelope.Message);

  const requestId = payload.requestId;
  const requestedBy = payload.requestedBy ?? "system";
  const requestedAt = new Date(payload.requestedAt);

  console.log(`Processing report request ${requestId}`);

  const client = await getPool().connect();
  try {
    await client.query("BEGIN");

    const items = await computeReports(client);
    const reportId = await insertHeader(
      client,
      requestId,
      requestedBy,
      requestedAt,
      items
    );
    await insertItems(client, reportId, items);

    await client.query("COMMIT");
    console.log(`Report ${reportId} stored with ${items.length} rows`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function computeReports(client) {
  const sql = `
    SELECT c.id,
           c.name,
           c.teacher_id,
           COUNT(e.id)::int AS enrollment_count
      FROM courses c
      LEFT JOIN enrollments e ON e.course_id = c.id
     GROUP BY c.id, c.name, c.teacher_id
     ORDER BY c.id
  `;
  const { rows } = await client.query(sql);
  const total = rows.reduce((acc, r) => acc + r.enrollment_count, 0);

  return rows.map((r) => ({
    courseId: r.id,
    courseName: r.name,
    teacherId: r.teacher_id,
    enrollmentCount: r.enrollment_count,
    enrollmentRate:
      total === 0 ? 0 : Math.round((r.enrollment_count * 10_000) / total) / 100,
  }));
}

async function insertHeader(client, requestId, requestedBy, requestedAt, items) {
  const totalEnrollments = items.reduce((acc, r) => acc + r.enrollmentCount, 0);
  const sql = `
    INSERT INTO course_reports
      (request_id, requested_by, requested_at, generated_at,
       total_courses, total_enrollments)
    VALUES ($1, $2, $3, NOW(), $4, $5)
    RETURNING id
  `;
  const { rows } = await client.query(sql, [
    requestId,
    requestedBy,
    requestedAt,
    items.length,
    totalEnrollments,
  ]);
  return rows[0].id;
}

async function insertItems(client, reportId, items) {
  if (items.length === 0) return;

  const values = [];
  const placeholders = items.map((r, i) => {
    const base = i * 6;
    values.push(
      reportId,
      r.courseId,
      r.courseName,
      r.teacherId,
      r.enrollmentCount,
      r.enrollmentRate
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`;
  });

  const sql = `
    INSERT INTO course_report_items
      (report_id, course_id, course_name, teacher_id,
       enrollment_count, enrollment_rate)
    VALUES ${placeholders.join(", ")}
  `;
  await client.query(sql, values);
}