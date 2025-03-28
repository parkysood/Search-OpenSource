import unittest
from backend_etl import transformer

# Testing the data transformation phase of ETL job
class TestCleanReposFunction(unittest.TestCase):
    
    # Test case: No repositories to clean
    def test_no_repos(self):
        repos = []
        result = transformer.clean_repos(repos)
        
        # Expected output: Empty list
        self.assertEqual(result, [])
    
    # Test case: Forked repositories
    def test_forked(self):
        repos = [{
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': True,         # Forked test repo
            'isArchived': False, 
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Empty list (should remove forked repos)
        self.assertEqual(result, [])
    
    # Test case: Archived repositories
    def test_archived(self):
        repos = [{
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': True,     # Archived test repo
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Empty list (should remove archived repos)
        self.assertEqual(result, [])
    
    # Test case: Repositories that don't have licenses
    def test_no_license(self):
        repos = [{
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False, 
            'url': 'test1', 
            'licenseInfo': None,    # No license info
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        },
        {
            'name': 'test2', 
            'description': 'test2', 
            'stargazerCount': 1234, 
            'pushedAt': 'test2', 
            'isFork': False, 
            'isArchived': False, 
            'url': 'test2', 
            'licenseInfo': "",    # No license info
            'owner': {'login': 'test2'}, 
            'primaryLanguage': {'name': 'Python'}
        },
        {
            'name': 'test2', 
            'description': 'test2', 
            'stargazerCount': 1234, 
            'pushedAt': 'test2', 
            'isFork': False, 
            'isArchived': False, 
            'url': 'test2', 
            'licenseInfo': [],    # No license info
            'owner': {'login': 'test2'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Empty list (should remove all instances of repos with no license metadata)
        self.assertEqual(result, [])
    
    # Test case: Repositories missing primary languages
    def test_missing_primary_language(self):
        repos = [{
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': None         # No primary language
        },
        {
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': ""         # No primary language
        },
        {
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': []         # No primary language
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: 4 repos, 3 with primary language metadata = None and 1 with original primary language
        self.assertEqual(len(result), 3)
        for i, cleaned_repo in enumerate(result):
            self.assertIsNone(cleaned_repo['language'])
    
    # Test case: Repository with primary language
    def test_primary_language(self):
        repos = [{
            'name': 'test1', 
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}       # Primary language = Python
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected result: Language of original repo should be present in cleaned version
        self.assertEqual(repos[0]['primaryLanguage']['name'], result[0]['language'])
        
    # Test case: Repository name
    def test_repo_name(self):
        repos = [{
            'name': 'test1',        # Repo name = test1
            'description': 'test1', 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Name should be unchanged
        self.assertEqual(repos[0]['name'], result[0]['name'])
    
    # Test case: Repository description missing
    def test_missing_description(self):
        repos = [{
            'name': 'test1', 
            'description': '',      # Missing description 
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Description should be empty
        self.assertEqual(result[0]['description'], "")
    
    # Test case: Repository description present
    def test_description(self):
        repos = [{
            'name': 'test1', 
            'description': 'testing repo',      # Description = testing repo
            'stargazerCount': 1234, 
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Description should be unchanged
        self.assertEqual(result[0]['description'], "testing repo")
    
    
    # Test case: Repository star count
    def test_repo_stars(self):
        repos = [{
            'name': 'test1', 
            'description': 'description',
            'stargazerCount': 1234,         # Star count = 1234
            'pushedAt': 'test1', 
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Star count should be unchanged
        self.assertEqual(result[0]['stars'], 1234)
    
    # Test case: Repository update time
    def test_repo_updatedAt(self):
        repos = [{
            'name': 'test1', 
            'description': 'description',
            'stargazerCount': 1234,
            'pushedAt': '2025-03-28T13:08:17Z',     # Last update to github repo
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Pushed time should be unchanged
        self.assertEqual(repos[0]['pushedAt'], result[0]['updated_at'])
    
    # Test case: Repository license
    def test_repo_license(self):
        repos = [{
            'name': 'test1', 
            'description': 'description',
            'stargazerCount': 1234,
            'pushedAt': 'testing',
            'isFork': False, 
            'isArchived': False,     
            'url': 'test1', 
            'licenseInfo': {'name': 'MIT'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Cleaned repo license is same as original repo license
        self.assertEqual(repos[0]['licenseInfo']['name'], result[0]['license'])
    
    # Test case: Repo URL
    def test_repo_url(self):
        repos = [{
            'name': 'test1', 
            'description': 'description',
            'stargazerCount': 1234,
            'pushedAt': 'testing',
            'isFork': False, 
            'isArchived': False,     
            'url': 'testing.com', 
            'licenseInfo': {'name': 'test1'}, 
            'owner': {'login': 'test1'}, 
            'primaryLanguage': {'name': 'Python'}
        }]
        
        result = transformer.clean_repos(repos)
        
        # Expected output: Cleaned repo URL is same as original repo URL
        self.assertEqual(repos[0]['url'], result[0]['url'])
    
if __name__ == '__main__':
    unittest.main()