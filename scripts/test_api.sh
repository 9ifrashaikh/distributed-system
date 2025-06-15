#!/bin/bash

BASE_URL="http://localhost:8080"

echo "Testing Storage API..."

# Test health check
echo "1. Health check:"
curl -X GET "$BASE_URL/health"
echo -e "\n"

# Test PUT object
echo "2. Uploading test file:"
echo "Hello, IBM Storage!" > test.txt
curl -X PUT "$BASE_URL/objects/test.txt" \
     -H "Content-Type: text/plain" \
     -H "User-ID: test-user" \
     --data-binary @test.txt
echo -e "\n"

# Test GET object
echo "3. Retrieving test file:"
curl -X GET "$BASE_URL/objects/test.txt"
echo -e "\n"

# Test LIST objects
echo "4. Listing all objects:"
curl -X GET "$BASE_URL/objects"
echo -e "\n"

# Test stats
echo "5. Getting stats:"
curl -X GET "$BASE_URL/stats"
echo -e "\n"

# Cleanup
rm test.txt