using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Hangfire.Azure.Queue;

internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
{
	private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);
	private readonly object cacheLock = new();
	private readonly PartitionKey partitionKey = new((int)DocumentTypes.Queue);
	private readonly List<string> queuesCache = new();
	private readonly CosmosDbStorage storage;
	private DateTime cacheUpdated;

	public JobQueueMonitoringApi(CosmosDbStorage storage)
	{
		this.storage = storage;
	}

	public IEnumerable<string> GetQueues()
	{
		lock (cacheLock)
		{
			// if cached and not expired return the cached item
			if (queuesCache.Count != 0 && cacheUpdated.Add(queuesCacheTimeout) >= DateTime.UtcNow) return queuesCache.ToList();

			QueryDefinition sql = new("SELECT DISTINCT VALUE doc['name'] FROM doc");

			List<string> result = storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
				.ToQueryResult()
				.ToList();

			queuesCache.Clear();
			queuesCache.AddRange(result);
			cacheUpdated = DateTime.UtcNow;

			return queuesCache;
		}
	}

	public int GetEnqueuedCount(string queue)
	{
		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.name = @name AND (NOT IS_DEFINED(doc.fetched_at) OR IS_NULL(doc.fetched_at))")
			.WithParameter("@name", queue);

		return storage.Container.GetItemQueryIterator<int>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
    {
        QueryDefinition sql = new QueryDefinition("SELECT VALUE doc.job_id FROM doc WHERE doc.name = @name AND (NOT IS_DEFINED(doc.fetched_at) OR IS_NULL(doc.fetched_at)) ORDER BY doc.created_on OFFSET @offset LIMIT @limit")
            .WithParameter("@name", queue)
            .WithParameter("@offset", from)
            .WithParameter("@limit", perPage);
        
        return storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey }).ToQueryResult();
    }

    public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
    {
        QueryDefinition sql = new QueryDefinition("SELECT VALUE doc.job_id FROM doc WHERE doc.name = @name AND IS_DEFINED(doc.fetched_at) AND NOT IS_NULL(doc.fetched_at) ORDER BY doc.created_on OFFSET @offset LIMIT @limit")
            .WithParameter("@name", queue)
            .WithParameter("@offset", from)
            .WithParameter("@limit", perPage);
        
        return storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey }).ToQueryResult();
    }

    public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
	{
        QueryDefinition sqlQueued = new QueryDefinition("SELECT VALUE COUNT(1) FROM doc WHERE doc.name = @name AND (NOT IS_DEFINED(doc.fetched_at) OR IS_NULL(doc.fetched_at))")
            .WithParameter("@name", queue);
        QueryDefinition sqlFetched = new QueryDefinition("SELECT VALUE COUNT(1) FROM doc WHERE doc.name = @name AND IS_DEFINED(doc.fetched_at) AND NOT IS_NULL(doc.fetched_at)")
            .WithParameter("@name", queue);

        int queued = storage.Container.GetItemQueryIterator<int>(sqlQueued, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
            .ToQueryResult()
            .FirstOrDefault();
        
        int fetched = storage.Container.GetItemQueryIterator<int>(sqlFetched, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
            .ToQueryResult()
            .FirstOrDefault();
        
		return new ValueTuple<int?, int?>(queued, fetched);
	}
}