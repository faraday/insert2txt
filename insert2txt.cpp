//
//
// insert2txt, by Pilho Kim, 2008.
//
// 
// Object: Convert the MySQL exported INSERT statements into raw data text objects.
// Usage: Insert2txt sourcefile [outputfile]
//		If [outputfile] is ommitted, then Insert2txt will dump the output to the screen
//
// Input file sample:
// 
// LOCK TABLES `pagelinks` WRITE;
// INSERT INTO `pagelinks` VALUES (0,0,'2008'),(0,0,'Commitment_scheme');
// INSERT INTO `pagelinks` VALUES (844,0,'William_the_Silent'),(844,0,'Working_class');
// INSERT INTO `pagelinks` VALUES (1141,0,'Optical_phenomenon'),(1141,0,'Paris'),(1141,0,'Physics');
//
// Output file sample:
// 0,0,'2008'
// 0,0,'Commitment_scheme'
// 844,0,'William_the_Silent'
// 844,0,'Working_class'
// 1141,0,'Optical_phenomenon'
// 1141,0,'Paris'
// 1141,0,'Physics'
//
// Compiler: MS VS 2005. Sorry to VS6 users. Yours do not support _stati64  
//			 and _ftelli64 that are necessary to handle big giga-byte files.
//

#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>
#include <inttypes.h>

#define	ATOI64(val)	strtoll(val, NULL, 10)
#define	ATOI64U(val)	strtoull(val, NULL, 10)

typedef int64_t __int64;

/* A `struct linebuffer' is a structure which holds a line of text.
   `readline' reads a line from a stream into a linebuffer
   and works regardless of the length of the line.  */

struct linebuffer {
	/* Note:  This is the number of bytes malloc'ed for `buffer'
	   It does not indicate `buffer's real length.
	   Instead, a null char indicates end-of-string.  */
	unsigned long size;
	char *buffer;
};

/* Read a line of text from STREAM into LINEBUFFER.
   Combine continuation lines into one line.
   Return the number of actual lines read (> 1 if hacked continuation lines).
 */

static unsigned int readline (struct linebuffer *linebuffer, FILE *stream)
{
	char *buffer = linebuffer->buffer;
	register char *p = linebuffer->buffer;
	register char *end = p + linebuffer->size;
	register long len, lastlen = 0;
	register char *p2;
	register unsigned int nlines = 0;
	register int backslash;

	*p = '\0';

	while (fgets (p, end - p, stream) != 0)
	{
		len = strlen (p);
		if (len == 0) {
			perror("warning: NUL character seen; rest of line ignored");
			p[0] = '\n';
			len = 1;
		}

		p += len;
		if (p[-1] != '\n')	{
			/* Probably ran out of buffer space.  */
			register unsigned int p_off = p - buffer;
			linebuffer->size *= 2;
			buffer = (char *) realloc (buffer, linebuffer->size);
			p = buffer + p_off;
			end = buffer + linebuffer->size;
			linebuffer->buffer = buffer;
			*p = '\0';
			lastlen = len;
			continue;
		}

		++nlines;

		if (len == 1 && p > buffer) {
			/* P is pointing at a newline and it's the beginning of
			the buffer returned by the last fgets call.  However,
			it is not necessarily the beginning of a line if P is
			pointing past the beginning of the holding buffer.
			If the buffer was just enlarged (right before the newline),
			we must account for that, so we pretend that the two lines
			were one line.  */
			len += lastlen;
		}

		lastlen = len;
		backslash = 0;
		for (p2 = p - 2; --len > 0; --p2) {
			if (*p2 == '\\')
				backslash = !backslash;
			else
				break;
		}

		if (!backslash) {
			p[-1] = '\0';
			if (p - p2 > 2)
				p[-2] = '\0'; /* kill one of multiple backslashes */
			break;
		}

		if (end - p <= 1) {
			/* Enlarge the buffer.  */
			register unsigned int p_off = p - buffer;
			linebuffer->size *= 2;
			buffer = (char *) realloc (buffer, linebuffer->size);
			p = buffer + p_off;
			end = buffer + linebuffer->size;
			linebuffer->buffer = buffer;
		}
	}

	if (ferror (stream)) {
		perror("Input stream error occurred!");
	}

	return nlines;
}

// Retrieve the file length.
__int64 GetFileLength(char *sFileName) {
   struct stat64 buf;
   int result;

   /* Get data associated with "stat.c": */
   result = stat64(sFileName, &buf );

   /* Check if statistics are valid: */
   if( result != 0 )
      perror( "Problem getting information" );

   return (__int64)buf.st_size;
}

// Find string and its location
long FindStr(char *sSource, char *sKey, long offset=0) {
	char *found = strstr((sSource+offset), sKey);

	if (found != NULL) {
		return (long)(found-sSource+1);
	}

	return -1;
}

int main(int argc, char* argv[])
{
	FILE *m_pFile;
	FILE *m_pSaveFile;

	char m_sFileName[1024];
	char m_sPos[5];
	// m_sSaveFileName is the name of the file to save. If iMaxFileSize is set, then assuming m_sOutputFileName as "output.txt",
	// then m_sSaveFile will be like output_1.txt, output_2.txt, ...
	char *pChar, sBuff[1024], m_sOutputFileName[1024], m_sOutputFileExtension[100], m_sSaveFileName[1024]; 
	long iDataCountPerLine = 0, iTotalDataCount = 0, iLineLength = 0;
	__int64 iFileLength = 0;
	long iStart = 0, iEnd = 0, iOldStart = 0;
	int iOutputFile = 0;
	double fPercent = 0.;
	long iLineCount = 0;
	struct linebuffer m_Line;

	// Sperate the result file by max size
	__int64 iMaxFileLength = -1; // If -1, then ignore
	__int64 iFileSizeSum=0;
	int iFileCount = 0;

	m_Line.size = 2485760; // Initial memeory allocation. 
	m_Line.buffer = (char *) calloc (m_Line.size, sizeof(char));

	if (argc < 2 ) {
		printf("Input file name is not specified\n\n");
		printf("Usage: Insert2txt sourcefile [outputfile] [maxsize_for_speration_in_megabytes]\n");
		printf("If [outputfile] is ommitted, then Insert2txt will dump the output to the screen.\n");
		printf("Use insert2txt -h to see helps.\n");
		return 0;
	}

	if (argc == 2 && strlen(argv[1]) == 2) {
		sprintf(m_sPos, "%s", argv[1]);

		if (!strcmp(m_sPos, "-h") || !strcmp(m_sPos, "-H") || !strcmp(m_sPos, "-?")) {
			printf("Insert2txt by Pilho Kim, 2008.\n");
			printf("\n");
			printf("Object: Convert the MySQL exported INSERT statements into raw data text objects.\n");
			printf("Usage: Insert2txt sourcefile [outputfile] [maxsize_for_speration_in_mb]\n");
			printf("If [outputfile] is omitted, then Insert2txt will dump the output to the screen\n");
			printf("\n");
			printf("Input file sample:\n");
			printf("\n");
			printf("LOCK TABLES `pagelinks` WRITE;\n");
			printf("INSERT INTO `pagelinks` VALUES (0,0,'2008'),(0,0,'Commitment_scheme');\n");
			printf("INSERT INTO `pagelinks` VALUES (844,0,'William_the_Silent'),(844,0,'Working_class');\n");
			printf("INSERT INTO `pagelinks` VALUES (1141,0,'Optical_phenomenon'),(1141,0,'Paris'),(1141,0,'Physics');\n");
			printf("\n");
			printf("Output file sample:\n");
			printf("\n");
			printf("0,0,'2008'\n");
			printf("0,0,'Commitment_scheme'\n");
			printf("844,0,'William_the_Silent'\n");
			printf("844,0,'Working_class'\n");
			printf("1141,0,'Optical_phenomenon'\n");
			printf("1141,0,'Paris'\n");
			printf("1141,0,'Physics'\n");
			printf("\n");
			printf("Compiler: MS VS 2005. \n");
			printf("\n");
			printf("Note: Sorry to VS6 users. Yours do not support _stati64 and _ftelli64 that are necessary to handle big giga-byte files.\n");
			return 0;
		}
	}

	sprintf(m_sFileName, "%s", argv[1]);

	if (argc >= 3) {
		sprintf(sBuff, "%s", argv[2]);
		sprintf(m_sSaveFileName, "%s", argv[2]);

		pChar = strrchr(sBuff, '.');

		if (pChar - sBuff > 0) {
			// If a file extension exists, then seprates
			strncpy(m_sOutputFileName, sBuff, pChar-sBuff);
			*(m_sOutputFileName+(int)(pChar-sBuff))=0x0;

			strncpy(m_sOutputFileExtension, pChar+1, strlen(pChar)-1);
			*(m_sOutputFileExtension+strlen(pChar)-1) = 0x0;
		}
		else {
			// If no extension, just copy the source
			sprintf(m_sOutputFileName, "%s", sBuff);
			sprintf(m_sOutputFileExtension, "");
		}

		iOutputFile = 1;

		// Create the output file
		if ((m_pSaveFile = fopen(sBuff, "wt")) == NULL) {
			printf("Output file creation error: %s\n", sBuff);
			return 0;
		}
		fclose(m_pSaveFile);

		printf("File loading...\n");
	}

	if (argc == 4) {
		sprintf(sBuff, "%s", argv[3]);
		iMaxFileLength = ATOI64(sBuff);

		// Convert to bytes
		iMaxFileLength = iMaxFileLength  * 1024 * 1024;
	} 
	else if (argc == 3) {
		sprintf(m_sSaveFileName, "%s", sBuff);
	}

	iFileLength = GetFileLength(m_sFileName);
	
	if ((m_pFile = fopen(m_sFileName, "rt")) == NULL) {
		printf("File read error: %s\n", m_sFileName);
		return 0;
	}

	// Main loop: Iterate the data file by line
	while (readline(&m_Line, m_pFile)) {
		// Calculate the progress
		fPercent = 100.*(long double) ftello64(m_pFile)/(long double)iFileLength;
		iLineLength = strlen(m_Line.buffer);
		iFileSizeSum += iLineLength;

		// Process data
		iStart = FindStr(m_Line.buffer, "INSERT");

		if (iStart >=0 && iStart < 6) {
			// For each line open and close the output file to save intermediate results for safety
			if (iOutputFile) {
				// Check file size
				if (iMaxFileLength > 0 && iFileSizeSum > iMaxFileLength) {
					sprintf(m_sSaveFileName, "%s_%d.%s", m_sOutputFileName, ++iFileCount, m_sOutputFileExtension);
					// Initialize
					iFileSizeSum = 0;
					
					// Create a file to write
					if ((m_pSaveFile = fopen(m_sSaveFileName, "wt")) == NULL) {
						printf("Output file write error: %s\n", m_sOutputFileName);
						return 0;
					}

					// Debug
					printf("Save to %s.\n", m_sSaveFileName);
				}
				else {
					// Open file to append
					if ((m_pSaveFile = fopen(m_sSaveFileName, "at")) == NULL) {
						printf("Output file write error: %s\n", m_sOutputFileName);
						return 0;
					}
				}

				printf("Read %d bytes, ", iLineLength);

			}

			// Init counter
			iDataCountPerLine = 0;

			// Find the start position
			iStart = FindStr(m_Line.buffer, "VALUES (", iStart) + 8;

			if (iStart <= 8) continue;

			do {
				// Find the end position
				iEnd = FindStr(m_Line.buffer, "),(", iStart+1) - 1;

				if (iEnd > iStart) {
					if (iOutputFile) {
						fwrite(m_Line.buffer + iStart-1, sizeof(char), iEnd-iStart+1, m_pSaveFile);
						fprintf(m_pSaveFile, "\n");
					}
					else {
						fwrite(m_Line.buffer + iStart-1, sizeof(char), iEnd-iStart+1, stdout);
						printf("\n");
					}

					iDataCountPerLine = iDataCountPerLine + 1;

					// Move offset
					iOldStart = iStart;
					iStart = iEnd + 4;

					if (iStart < iOldStart) {
						printf("Some error.. \n");
					}
				}
				else {
					// Find the last data set in one data line
					iEnd = FindStr(m_Line.buffer, ");", iStart+1) - 1;

					if (iEnd > iStart) {
						if (iOutputFile) {
							fwrite(m_Line.buffer + iStart-1, sizeof(char), iEnd-iStart+1, m_pSaveFile);
							fprintf(m_pSaveFile, "\n");
						}
						else {
							fwrite(m_Line.buffer + iStart-1, sizeof(char), iEnd-iStart+1, stdout);
							printf("\n");
						}

						iDataCountPerLine = iDataCountPerLine + 1;

						// Move the offset to the end
						iOldStart = iStart;
						iStart = iLineLength;
					}
				}
			} while (iStart < iLineLength - 3);

			iTotalDataCount = iTotalDataCount + iDataCountPerLine;

			// For each line open and close the output file to save intermediate results for safety
			if (iOutputFile) {
				fclose(m_pSaveFile);

				// Print the debug info
				printf("(%.3f%%) : %d data, %d total.\n", fPercent, iDataCountPerLine, iTotalDataCount);
			}

			iDataCountPerLine = 0;
		}
	};

	fclose(m_pFile);

	printf("Success.");

	return 0;
}

