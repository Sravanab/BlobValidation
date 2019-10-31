using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Globalization;

namespace BlobFileNameChecker
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {

            log.LogInformation("C# HTTP trigger function processed a request.");

            #region StorageAccount

            const string blobContainerName = "rawcsvfiles";

            List<string> _inputFiles = new List<string> { "Claims", "CoverPremium", "PolicyTransaction", "Quotes" };

            string _foldername = DateTime.Now.ToString("dd-MM-yyyy", CultureInfo.InvariantCulture);


        
            CloudBlobClient _blobClient =await GetBlobClient();



            CloudBlobContainer _blobContainer = _blobClient.GetContainerReference(blobContainerName);

            await _blobContainer.CreateIfNotExistsAsync();

            await _blobContainer.SetPermissionsAsync(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });


            CloudBlobDirectory dira = _blobContainer.GetDirectoryReference(_foldername);

            Task<BlobResultSegment> res = dira.ListBlobsSegmentedAsync(new BlobContinuationToken());

            IEnumerable<IListBlobItem> result = res.Result.Results;

            bool _isAlreadyExist = false;

            System.Text.StringBuilder stringbuilder = new System.Text.StringBuilder();

            if (result.GetEnumerator().MoveNext())
            {
                foreach (CloudBlockBlob blob in result)
                {

                    if (blob.GetType() == typeof(CloudBlockBlob))
                    {
                        string[] s = blob.Name.Split('/');


                        if (s.Length != 2)
                        {
                            log.LogError($"file name {blob.Name} is not in the correct format");
                            Uri _invalid_destBlob = await BlobCopy(blob, log, "invalid");
                            continue;
                        }
                        string[] date= null;

                        if (s[1].Contains("_v"))
                        {
                           
                              date = s[1].Split('_');
                              string sr = date[0]+"_" + date[2]+ "_" + date[3]+ "_" + date[4];
                              date = sr.Split('_');
                        }
                        else
                        {
                             date = s[1].Split('_');
                        }

                        if (date.Length != 4 || date[1].Length != 4 || date[2].Length != 2 || date[3].Split('.')[0].Length != 2)
                        {
                            log.LogError($"file name {blob.Name} is not in the correct date format. It should be in  dd-MM-YYYY /(filename_yyyy_MM_dd or filename_v(version number)_yyyy_MM_dd) format");
                            Uri _invalid_destBlob = await BlobCopy(blob, log, "invalid");
                            continue;
                        }

                        try
                        {

                            string year = date[1];
                            string month = date[2];
                            string day = date[3].Split('.')[0];
                            string _fileDate = day + "-" + month + "-" + year;
                            DateTime fileCorrectDate = DateTime.ParseExact(_fileDate, "dd-MM-yyyy",null);
                            DateTime currentDate = DateTime.ParseExact(DateTime.Now.ToString("dd-MM-yyyy"),"dd-MM-yyyy",null );
                            int t = DateTime.Compare(currentDate,fileCorrectDate);

                            if (_inputFiles.Contains(s[1].Split('_')[0]) && Path.GetExtension(s[1]) == ".csv" && blob.Properties.Length >= 0)
                            {
                                if (t > 0)
                                {
                                    _isAlreadyExist = await CheckDuplicateInGivenDate(_fileDate, blob.Name);
                                }
                                if (_isAlreadyExist)
                                {
                                    Console.BackgroundColor = (ConsoleColor.Red);
                                    log.LogWarning($"{blob.Uri}  already exist in {_fileDate} date");
                                    stringbuilder.AppendFormat($"{blob.Uri}  already exist in {_fileDate} date",Environment.NewLine);
                                    Console.BackgroundColor = (ConsoleColor.Blue);

                                }
                                else
                                {
                                    Uri __destBlob = await BlobCopy(blob, log, "valid");
                                   stringbuilder.AppendFormat($"{blob.Uri} is copied to {__destBlob}",Environment.NewLine);
                                   log.LogInformation($"{blob.Uri} is copied to {__destBlob}");
                                }
                            
                            }
                            else
                            {
                                Uri _invalid_destBlob = await BlobCopy(blob, log, "invalid");
                                stringbuilder.AppendFormat($"{blob.Uri} is copied to {_invalid_destBlob}",Environment.NewLine);
                                log.LogInformation($"{blob.Uri} is copied to {_invalid_destBlob}");
                            }

                        }
                        catch (Exception ex)
                        {
                            log.LogError($"{ex.Message}");
                        }

                    }

                }
            }
            else
            {
                log.LogError($"Please make sure the files are there in the given storage account path {blobContainerName} / {_foldername}");
                return new BadRequestObjectResult($"Please make sure the files are there in the given storage account path {blobContainerName} / {_foldername}");
            }
            #endregion


            return stringbuilder.Length > 0
        ? (ActionResult)new OkObjectResult(stringbuilder)
        : new BadRequestObjectResult($"Please make sure that files are there in the given storage account path {blobContainerName} / {_foldername} or files are in correct format or not");

        }

        private static async Task<bool> CheckDuplicateInGivenDate(string fileDate, string fileName)
        {
            const string blobContainerName = "rawcsvfiles";
           // CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=rsadevblobstorage;AccountKey=TUHaMlHc5u0pU+UvtneR109q4npWVRE8l4H34ncA/RzvEyrZfo/f2ard4QQEcGpEUUhspwulFndoPoQq1/wQvQ==;EndpointSuffix=core.windows.net");

            // Create a blob client for interacting with the blob service.
            CloudBlobClient _blobClient =await GetBlobClient();
            CloudBlobContainer _blobContainer = _blobClient.GetContainerReference(blobContainerName);
            await _blobContainer.SetPermissionsAsync(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });


            CloudBlobDirectory dira = _blobContainer.GetDirectoryReference(fileDate);

            Task<BlobResultSegment> res = dira.ListBlobsSegmentedAsync(new BlobContinuationToken());

            IEnumerable<IListBlobItem> result = res.Result.Results;
            List<string> blobs = new List<string>();

            if (result.GetEnumerator().MoveNext())
            {
                foreach (CloudBlockBlob blob in result)
                {

                    if (blob.GetType() == typeof(CloudBlockBlob))
                    {
                        blobs.Add(blob.Name.Split('/')[1]);
                    }
                }

                if (blobs.Contains(fileName.Split('/')[1]))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        public static async Task<Uri> BlobCopy(CloudBlockBlob srcBlob, ILogger log, string fileType)
        {
            try
            {
                CloudBlockBlob _destBlob;
                //CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=rsadevblobstorage;AccountKey=TUHaMlHc5u0pU+UvtneR109q4npWVRE8l4H34ncA/RzvEyrZfo/f2ard4QQEcGpEUUhspwulFndoPoQq1/wQvQ==;EndpointSuffix=core.windows.net");
                CloudBlobClient cloudBlobClient = await GetBlobClient();
                    //.CreateCloudBlobClient();
                string strContainerName = fileType;
                CloudBlobContainer _destContainer = cloudBlobClient.GetContainerReference(strContainerName);
                await _destContainer.CreateIfNotExistsAsync();

                if (srcBlob == null)
                {
                    throw new Exception("Source blob cannot be null.");
                }

                //Copy source blob to destination container
                string name = DateTime.Now.ToString("yyyy") + "/" + DateTime.Now.ToString("MM") + "/" + DateTime.Now.ToString("dd") + "/" + srcBlob.Name.Split('/')[1].Split('_')[0] + "/" + Convert.ToString(srcBlob.Name.Split('/')[1]);
                _destBlob = _destContainer.GetBlockBlobReference(name);
                await _destBlob.StartCopyAsync(srcBlob);
                return _destBlob.Uri;
            }

            catch (Exception ex)
            {
                log.LogError(ex.Message);
                throw ex;
            }




        }

        private static async Task<CloudBlobClient>GetBlobClient()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=rsadevblobstorage;AccountKey=TUHaMlHc5u0pU+UvtneR109q4npWVRE8l4H34ncA/RzvEyrZfo/f2ard4QQEcGpEUUhspwulFndoPoQq1/wQvQ==;EndpointSuffix=core.windows.net");

            // Create a blob client for interacting with the blob service.
            CloudBlobClient _blobClient = storageAccount.CreateCloudBlobClient();

            return _blobClient;
        }
    }
}
