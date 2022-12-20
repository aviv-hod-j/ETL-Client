import json

class ETLClient:
    def run(self, service, max_requests):
        documents = []
        error_count = 0
        for i in range(0, max_requests):
            event = dict()
            while not event:
                try:
                    event = service.handle_request()
                except RetryImmediatelyError:
                    error_count += 1
                except Exception:
                    event['operation'] = 'Error'
                    error_count += 1

            if event['operation'] == 'add':
                documents.append(event['document'])

            elif event['operation'] == 'update':
                index = self.find_document_idx_by_id(documents, event['document']['id'])
                if index:
                    documents[index]['data'] = event['document']['data']
                else:
                    print("Document not found")
            elif event['operation'] == 'delete':
                index = self.find_document_idx_by_id(documents, event['document']['id'])
                if index:
                    documents.pop(index)
                else:
                    print("Document not found")

        for i in range(len(documents)):
            documents[i]['data'] = self.clean_data(documents[i]['data'])

        summary = {'doc-count': len(documents),
                   'error-count': error_count,
                   'docs': {document['id']: document['data'] for document in documents}}

        return json.dumps(summary)

    @staticmethod
    def find_document_idx_by_id(documents, doc_id):
        """
        finds index of document in documents list by document id
        :param documents: list of documents processed by client
        :param doc_id: document id to find
        :return: index of document in list
        """
        for i in range(len(documents)):
            if documents[i]['id'] == doc_id:
                return i
        return None

    @staticmethod
    def clean_data(data):
        """
        Removes conjunctions from data and turns it to lowercase
        :param data: str data to clean
        :return: cleaned document list of words
        """
        unwanted_words_list = ['and', 'or', 'not', 'but', 'to', 'in']
        splited_data = data.split(' ')
        cleaned_data = list()
        for word in splited_data:
            if word not in unwanted_words_list:
                cleaned_data.append(word.lower())
        return cleaned_data
