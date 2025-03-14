import ray

# Initialize Ray
ray.init()

@ray.remote
def hello_world():
    return "Hello, World from a Ray worker!"

# Execute the remote function
result_ids = [hello_world.remote() for _ in range(4)]

# Get the results
results = ray.get(result_ids)

# Print the results
for result in results:
    print(result)
