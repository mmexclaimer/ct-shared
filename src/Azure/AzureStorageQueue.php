<?php

namespace src\Azure;

use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Queue\Models\CreateQueueOptions;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions;
use MicrosoftAzure\Storage\Queue\Models\PeekMessagesOptions;
use MicrosoftAzure\Storage\Queue\Models\QueueMessage;
use MicrosoftAzure\Storage\Queue\QueueRestProxy;

class AzureStorageQueue
{
    /**
     * @var string
     */
    private string $connectionString;

    /**
     * @var QueueRestProxy
     */
    private $queueClient;

    /**
     * @var string|null
     */
    protected ?string $queueName;

    /**
     * @param string|null $queueName // Creates a Storage Queue with the specified name if it doesn't exist yet
     */
    public function __construct(?string $queueName = null)
    {
        $this->connectionString = "DefaultEndpointsProtocol=https;" .
                                  "AccountName=" . env('AZURE_QUEUE_ACCOUNT_NAME') . ";" .
                                  "AccountKey=" . env('AZURE_QUEUE_ACCOUNT_KEY') . ";" .
                                  "EndpointSuffix=core.windows.net";

        $this->queueClient = QueueRestProxy::createQueueService($this->connectionString);
        $this->queueName   = $queueName;


        // For some reason, during the class construction, the queueName is always NULL
        // That prevents the queue from being created at this point
        // $this->createQueue();
    }

    /**
     * @param array $queueOptions
     *
     * @return bool
     */
    public function createQueue(array $queueOptions = []): bool
    {
        $createQueueOptions = new CreateQueueOptions();
        foreach ($queueOptions as $key => $value) {
            $createQueueOptions->addMetaData($key, $value);
        }

        try {
            // Create queue.
            $this->queueClient->createQueue($this->queueName, $createQueueOptions);
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return false;
        }

        return true;
    }

    /**
     * @param string $message
     *
     * @return bool
     */
    public function addMessageToQueue(string $message): bool
    {
        try {
            // make sure the queue exists
            $this->createQueue();
            // Create message.
            $this->queueClient->createMessage($this->queueName, $message);
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return false;
        }

        return true;
    }

    /**
     * @return false|int
     */
    public function getApproximateQueueLength()
    {
        try {
            // make sure the queue exists
            $this->createQueue();

            // Get queue metadata.
            $queueMetadata = $this->queueClient->getQueueMetadata($this->queueName);
            return $queueMetadata->getApproximateMessageCount();
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return false;
        }
    }

    /**
     * @param int $numberOfMessages
     *
     * @return QueueMessage[]|null
     */
    public function peekAtMessages(int $numberOfMessages = 1): ?array
    {
        // make sure the queue exists
        $this->createQueue();

        // OPTIONAL: Set peek message options.
        $messageOptions = new PeekMessagesOptions();
        $messageOptions->setNumberOfMessages($numberOfMessages);

        try {
            $peekMessagesResult = $this->queueClient->peekMessages($this->queueName, $messageOptions);
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return null;
        }

        return $peekMessagesResult->getQueueMessages();
    }

    /**
     * @return bool
     */
    public function dequeueNextMessage(): bool
    {
        // make sure the queue exists
        $this->createQueue();

        $message = $this->getMessagesList($this->queueName);
        if (count($message)) {
            $message = $message[0];
        }

        /* ---------------------
            Process message.
           --------------------- */

        // Get message ID and pop receipt.
        $messageId  = $message->getMessageId();
        $popReceipt = $message->getPopReceipt();

        return $this->dequeueMessage($this->queueName, $messageId, $popReceipt);
    }

    /**
     * @param int $numberOfMessages
     *
     * @return QueueMessage[]
     */
    public function getMessagesList(int $numberOfMessages = 1): array
    {
        // make sure the queue exists
        $this->createQueue();

        $options = new ListMessagesOptions();
        $options->setNumberOfMessages($numberOfMessages);

        $listMessagesResult = $this->queueClient->listMessages($this->queueName, $options);
        return $listMessagesResult->getQueueMessages();
    }

    /**
     * @param string      $messageId
     * @param string|null $popReceipt
     *
     * @return bool
     */
    public function dequeueMessage(string $messageId, string $popReceipt): bool
    {
        // make sure the queue exists
        $this->createQueue();

        try {
            // Delete message.
            $this->queueClient->deleteMessage($this->queueName, $messageId, $popReceipt);
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return false;
        }

        return true;
    }

    /**
     * @return int|null
     */
    public function getQueueLength(): ?int
    {
        // make sure the queue exists
        $this->createQueue();

        try {
            // Get queue metadata.
            $queueMetadata           = $this->queueClient->getQueueMetadata($this->queueName);
            $approximateMessageCount = $queueMetadata->getApproximateMessageCount();
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code          = $e->getCode();
            $error_message = $e->getMessage();
            echo $code . ": " . $error_message . "<br />";

            return null;
        }

        return $approximateMessageCount;
    }

    /**
     * @return bool
     */
    public function deleteQueue(): bool
    {
        try {
            // Delete queue.
            $this->queueClient->deleteQueue($this->queueName);
        } catch (ServiceException $e) {
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // https://msdn.microsoft.com/library/azure/dd179446.aspx
            $code         = $e->getCode();
            $errorMessage = $e->getMessage();
            echo $code . ": " . $errorMessage . "<br />";

            return false;
        }

        return true;
    }
}