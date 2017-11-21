#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"

using System;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
{
    string jsonContent = await req.Content.ReadAsStringAsync();
    dynamic data = JsonConvert.DeserializeObject(jsonContent);

    log.Info($"WebHook was triggered with the following request data: {data}");

    string sourceStorageConnectionString = "<replace with your connection string for source storage account>";
    string destinationStorageConnectionString = "<replace with your connection string for destination storage account>";

    //Get references to the containers
    string[] subjectSplit = data[0].subject.ToString().Split('/');
    int containerPosition = Array.IndexOf(subjectSplit, "containers");
    string container = subjectSplit[containerPosition + 1];
    log.Info($"Container is: {container}");
    string archiveContainer = container + "-archive";

    //Get reference to the blob name
    int blobPosition = Array.IndexOf(subjectSplit, "blobs");
    string fileName = subjectSplit[blobPosition + 1];
    log.Info($"Blob name is: {fileName}");

    // Set up source storage account connection
    CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(sourceStorageConnectionString);
    CloudBlobClient sourceStorageClient = sourceAccount.CreateCloudBlobClient();
    var sourceContainer = sourceStorageClient.GetContainerReference(container);

    // Set up destination storage account creation
    CloudStorageAccount destinationAccount = CloudStorageAccount.Parse(destinationStorageConnectionString);
    CloudBlobClient destinationStorageClient = destinationAccount.CreateCloudBlobClient();
    // Create the replica container if it doesn't exist
    var destinationContainer = destinationStorageClient.GetContainerReference(container);
    try
    {
        await destinationContainer.CreateIfNotExistsAsync();
    }
    catch (Exception e)
    {
        log.Error(e.Message);
    }
    // Create the replica archive container if it doesn't exist
    var destinationArchiveContainer = destinationStorageClient.GetContainerReference(archiveContainer);
    try
    {
        await destinationArchiveContainer.CreateIfNotExistsAsync();
    }
    catch (Exception e)
    {
        log.Error(e.Message);
    }

    if (data[0].eventType == "Microsoft.Storage.BlobCreated")
    {
        //A new blob was created, replicate the blob to another storage account
        log.Info($"EventType: {data[0].eventType}");
        // Get SAS for the blob so that we can access a private blob
        CloudBlockBlob sourceBlobVersionCheck = sourceContainer.GetBlockBlobReference(fileName);
        // Get the LastModified property to add to the versioned file name
        await sourceBlobVersionCheck.FetchAttributesAsync();
        DateTime lastModDT = Convert.ToDateTime(sourceBlobVersionCheck.Properties.LastModified.ToString());
        string lastModified = lastModDT.ToString("yyyyMMdd-HHmmsszz");
        string versionedFileName = fileName + "." + lastModified;
        // Get references to the source and replica blobs
        CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(fileName);
        CloudBlockBlob destinationArchiveBlob = destinationArchiveContainer.GetBlockBlobReference(versionedFileName);
        string sourceBlobUriString = GetBlobSasUri(sourceContainer, fileName, null);
        Uri sourceBlobUri = new Uri(sourceBlobUriString);
        CloudBlockBlob sourceBlob = new CloudBlockBlob(sourceBlobUri);
        if (await sourceBlob.ExistsAsync())
        {
            // Create the replica
            log.Info($"Copying {sourceBlob.Uri.ToString()} to {destinationBlob.Uri.ToString()}");
            try
            {
                await destinationBlob.StartCopyAsync(sourceBlob);
            }
            catch (Exception e)
            {
                log.Error(e.Message);
            }
            // Create the archive replica
            log.Info($"Copying {sourceBlob.Uri.ToString()} to {destinationArchiveBlob.Uri.ToString()}");
            try
            {
                await destinationArchiveBlob.StartCopyAsync(sourceBlob);
            }
            catch (Exception e)
            {
                log.Error(e.Message);
            }

            return req.CreateResponse(HttpStatusCode.OK, new
            {
                body = $"Copied blob {sourceBlob.Uri.ToString()} to {destinationBlob.Uri.ToString()} and created a versioned replica at {destinationArchiveBlob.Uri.ToString()}"
            });
        }
        else
        {
            log.Info("Source blob does not exist, no copy made");
            return req.CreateResponse(HttpStatusCode.OK, new
            {
                body = $"Blob {sourceBlob.Uri.ToString()} was not copied as it did not exist at run time"
            });
        }
    }
    else if (data[0].eventType == "Microsoft.Storage.BlobDeleted")
    {
        //Blob was deleted, delete the replica if it exists
        log.Info($"EventType: {data[0].eventType}");
        CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(fileName);
        try
        {
            await destinationBlob.DeleteIfExistsAsync();
        }
        catch (Exception e)
        {
            log.Error(e.Message);
        }

        return req.CreateResponse(HttpStatusCode.OK, new
        {
            body = $"Deleted blob {destinationBlob.Uri.ToString()}"
        });
    }

    return req.CreateResponse(HttpStatusCode.OK, new
    {
        body = $"ReplicateBlob Azure Function"
    });
}

//source: https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1
//create a sas service on a blob section
private static string GetBlobSasUri(CloudBlobContainer container, string blobName, string policyName = null)
{
    string sasBlobToken;

    // Get a reference to a blob within the container.
    // Note that the blob may not exist yet, but a SAS can still be created for it.
    CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

    if (policyName == null)
    {
        // Create a new access policy and define its constraints.
        // Note that the SharedAccessBlobPolicy class is used both to define the parameters of an ad-hoc SAS, and
        // to construct a shared access policy that is saved to the container's shared access policies.
        SharedAccessBlobPolicy adHocSAS = new SharedAccessBlobPolicy()
        {
            // When the start time for the SAS is omitted, the start time is assumed to be the time when the storage service receives the request.
            // Omitting the start time for a SAS that is effective immediately helps to avoid clock skew.
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
            Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Create
        };

        // Generate the shared access signature on the blob, setting the constraints directly on the signature.
        sasBlobToken = blob.GetSharedAccessSignature(adHocSAS);
    }
    else
    {
        // Generate the shared access signature on the blob. In this case, all of the constraints for the
        // shared access signature are specified on the container's stored access policy.
        sasBlobToken = blob.GetSharedAccessSignature(null, policyName);
    }

    // Return the URI string for the container, including the SAS token.
    return blob.Uri + sasBlobToken;
}
