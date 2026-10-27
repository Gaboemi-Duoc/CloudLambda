process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

exports.handler = async (event) => {
    for (const record of event.Records) {
        try {
            const snsMessage = JSON.parse(record.body); 
            const payload = typeof snsMessage.Message === 'string' 
                ? JSON.parse(snsMessage.Message) 
                : snsMessage.Message;

            console.log(`Processing notification: ${payload.message}`);

            // Make sure you are using the exact URL
            const response = await fetch('https://172-233-15-71.ip.linodeusercontent.com/api/saas/notices', {
                method: 'POST',
                headers: {
                    'Authorization': `ApiKey ${process.env.NOTIFICATION_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ text: payload.message }) 
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            console.log(`Notification sent successfully. Status: ${response.status}`);
            
        } catch (error) {
            console.error('Failed to process notification:', error);
            throw error; 
        }
    }
    
    return { statusCode: 200, body: 'Notifications processed' };
};