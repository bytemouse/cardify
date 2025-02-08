# Book to Quiz Generator Design Document

## Overview
An application that converts books into markdown format and generates educational question-answer pairs using Google's Gemini API.

## Core Components

### Text Extraction Module
- AI basd PDF to Markdown
- [marker](https://github.com/VikParuchuri/marker)
- Command: `marker_single file.pdf --output_format markdown --paginate_output --output_dir . --use_llm --disable_image_extraction --debug`

### Content Processing
- Text chunking algorithm to break content into digestible segments
- Metadata preservation for chapter/section tracking
- use [langchain-text-splitter](https://python.langchain.com/api_reference/text_splitters/markdown/langchain_text_splitters.markdown.ExperimentalMarkdownSyntaxTextSplitter.html)

### Question Generation
- Gemini API integration for Q&A generation
- Prompt templates optimized for educational content
- Rate limiting and batch processing for API calls


