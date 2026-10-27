exports.handler = async (event) => {
    // SQS sends a batch of records
    for (const record of event.Records) {
        try {
            // SNS wraps the message inside the SQS body
            const snsMessage = JSON.parse(record.body); 
            
            // If SNS sent a stringified JSON object, we need to parse it again
            const payload = typeof snsMessage.Message === 'string' 
                ? JSON.parse(snsMessage.Message) 
                : snsMessage.Message;

            console.log(`Processing notification: ${payload.message}`);

            const response = await fetch('https://172-233-15-71.ip.linodeusercontent.com/api/saas/notices', {
                method: 'POST',
                headers: {
                    'Authorization': `ApiKey ${process.env.NOTIFICATION_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                // The API expects the text inside a "text" key
                body: JSON.stringify({ text: payload.message }) 
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            console.log(`Notification sent successfully. Status: ${response.status}`);
            
        } catch (error) {
            console.error('Failed to process notification:', error);
            // Throwing the error causes SQS to retry the message (or send to DLQ)
            throw error; 
        }
    }
    
    return { statusCode: 200, body: 'Notifications processed' };
};