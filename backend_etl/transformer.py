def clean_repos(repos):
    cleaned = []    # Store cleaned repository data
    
    for repo in repos:
        # Ignore forks, archived repos, poorly structured repositories (no license, )
        if repo['isFork'] or repo['isArchived'] or not repo['licenseInfo']:
            continue
        
        cleaned.append({
            "name": repo["name"],
            "description": repo["description"] or "",
            "owner": repo["owner"]["login"],
            "language": repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
            "stars": repo["stargazerCount"],
            "updated_at": repo["pushedAt"],
            "license": repo["licenseInfo"]["name"],
            "url": repo["url"],
            "embedding": []
        })
    
    return cleaned