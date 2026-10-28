process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

exports.handler = async (event) => {
    for (const record of event.Records) {
        try {
            const snsMessage = JSON.parse(record.body); 
            const payload = typeof snsMessage.Message === 'string' 
                ? JSON.parse(snsMessage.Message) 
                : snsMessage.Message;

            console.log(`Processing notification: ${payload.message}`);

            // 1. Send to the Linode Notice API (as before)
            const response = await fetch('https://172-233-15-71.ip.linodeusercontent.com/api/saas/notices', {
                method: 'POST',
                headers: {
                    'Authorization': `ApiKey ${process.env.NOTIFICATION_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ text: payload.message }) 
            });

            if (!response.ok) {
                console.error(`Notice API error! status: ${response.status}`);
            } else {
                console.log(`Notice sent successfully.`);
            }

            // 2. NEW: If this is a REPORT, send an email
            if (payload.type === 'REPORT_GENERATED' && payload.data && payload.data.requestedBy) {
                const emailTo = payload.data.requestedBy;
                const reportRows = payload.data.reportData || [];

                // Build the email body
                let emailText = `Hello!\n\nHere is the course report you requested:\n\n`;
                let emailHtml = `<h2>📊 Course Report</h2><ul>`;

                reportRows.forEach(c => {
                    emailText += `- ${c.course_name}: ${c.student_count} students, ${c.task_count} tasks\n`;
                    emailHtml += `<li><strong>${c.course_name}</strong>: ${c.student_count} students, ${c.task_count} tasks</li>`;
                });
                
                emailHtml += `</ul><p>Generated automatically by CloudEvaluation.</p>`;

                console.log(`Sending report email to: ${emailTo}`);

                const emailResponse = await fetch('https://app.claudioviajando.com/api/saas/send-email', {
                    method: 'POST',
                    headers: {
                        'Authorization': `ApiKey ${process.env.NOTIFICATION_API_KEY}`, // Assuming same API key
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        to: emailTo,
                        subject: 'Your Course Analytics Report',
                        text: emailText,
                        html: emailHtml
                    })
                });

                if (!emailResponse.ok) {
                    const errorText = await emailResponse.text();
                    throw new Error(`Email API error! status: ${emailResponse.status} - ${errorText}`);
                }
                
                console.log(`Email sent successfully to ${emailTo}`);
            }
            
        } catch (error) {
            console.error('Failed to process notification:', error);
            throw error; 
        }
    }
    
    return { statusCode: 200, body: 'Notifications processed' };
};