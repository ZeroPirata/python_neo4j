from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable, ConstraintError, BrokenRecordError


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def criar_objeto(self):
        with self.driver.session(database="neo4j") as session:
            print("Produto | Vendedor | Pessoa")
            objeto_nome = input("O que quer criar?: ")
            session.execute_write(self._criar_objeto, objeto_nome.lower())

    def relacionamentos(self):
        with self.driver.session(database="neo4j") as session:
            print("Criar Relação com: Vendedor | Pessoa")
            escolha = input("Escolha: ")
            escolha = escolha.lower()
            if escolha == "vendedor":
                result = session.execute_write(self._vendedor_produto)
                for row in result:
                    print(
                        "Criando relação entre: {p1}, {p2}".format(
                            p1=row["p1"], p2=row["p2"]
                        )
                    )
            elif escolha == "pessoa":
                result = session.execute_write(self._pessoa_produto)
                for row in result:
                    print(
                        "Criando relação entre: {p1}, {p2}".format(
                            p1=row["p1"], p2=row["p2"]
                        )
                    )

    def querys(self):
        with self.driver.session(database="neo4j") as session:
            print("Produto | Vendedor | Pessoa | Geral")
            findQuery = input("Opção: ")
            findQuery = findQuery.lower()
            if findQuery == "pessoa" or findQuery == "vendedor":
                print("Com relação | Sem relação")
                relacao = input("Com / Sem")
                if relacao.lower() == "com":
                    result = session.execute_read(self._querys_relacao, findQuery, relacao)
                    for row in result:
                        print(findQuery + " encontrado \nnome: {alvo} \nproduto:{produto}".format(alvo=row['Nome'], produto=row['Produto']))
                else:
                    result = session.execute_read(
                        self._querys_produto_vendedor, findQuery
                    )
                for row in result:
                    print(
                        findQuery
                        + " encontrado \nnome: {row[1]} \nemail: {row[0]}\nidade: {row[2]}".format(
                            row=row
                        )
                    )
            if findQuery == "produto":
                result = session.execute_read(self._querys_produtos, findQuery)
                for row in result:
                    print(
                        "Produto encontrada \nnome: {row[1]} \nquantidade: {row[0]}\nvalor: {row[2]}".format(
                            row=row
                        )
                    )
            if findQuery == "geral":
                print("Produto | Pessoa | Vendedor")
                escolha = input("Escolha: ")
                escolha = escolha.lower()
                if escolha == "produto" or escolha == "vendedor":
                    result = session.execute_read(self._querys, escolha)
                    for row in result:
                        print(
                            "\n"
                            + findQuery
                            + " encontrado \nnome: {row[1]} \nemail: {row[0]}\nidade: {row[2]}".format(
                                row=row
                            )
                        )

    def delete(self):
        with self.driver.session(database="neo4j") as session:
            print("Produto | Vendedor | Pessoa")
            findQuery = input("Opção: ")
            findQuery = findQuery.lower()
            relacao = input("Possue relação com outro? Sim/Nao: ")
            relacao.lower()
            if relacao == "sim":
                session.execute_write(self._delete_relacao, findQuery)
            else:
                session.execute_write(self._delete, findQuery)

    def atualiazr(self):
        with self.driver.session(database="neo4j") as session:
            findQuery = input("Opção: ")
            findQuery = findQuery.lower()
            session.execute_write(self._atualizar, findQuery)

    @staticmethod
    def _querys_relacao(banco, tipo, relacao):
        relacao = relacao
        if tipo == "vendedor":
            relacao = "vende"
        if tipo == "pessoa":
            relacao = "Comprou"
        alvo = input("Nome do alvo: ")
        relacao = ":" + relacao
        query = (
            "MATCH (obj:" + tipo + ")"
            "MATCH (pro:produto) "
            "WHERE obj.nome = $alvo "
            "AND (obj)-[" + relacao + "]->(pro) "
            "RETURN obj.nome as Nome , pro.nome as Produto "
        )
        resultado = banco.run(query, alvo=alvo)
        return [[row['Nome'], row['Produto']] for row in resultado]

    @staticmethod
    def _atualizar(banco, tipo):
        if tipo == "pessoa" or tipo == "vendedor":
            alvo = input("Nome da pessoa/vendedor a ser atualizado: ")
            print("Email | Nome | Idade")
            item = input("Digite o item a atualizar: ")
            novo_valor = input("Digite a nova informação: ")
            objeto_atualizador = "p." + item
            query = (
                "MATCH (p:" + tipo + ") "
                "WHERE p.nome = $alvo "
                "SET " + objeto_atualizador + " = $novo_valor"
            )
            banco.run(query, alvo=alvo, item=item, novo_valor=novo_valor)
        if tipo == "produto":
            alvo = input("Nome do produto a ser atualizado: ")
            print("Nome | Quantidade | Valor")
            item = input("Digite o item a atualizar: ")
            novo_valor = input("Digite a nova informação: ")
            objeto_atualizador = "p." + item
            query = (
                "MATCH (p:" + tipo + ") "
                "WHERE p.nome = $alvo "
                "SET " + objeto_atualizador + " = $novo_valor"
            )
            banco.run(query, alvo=alvo, item=item, novo_valor=novo_valor)

    @staticmethod
    def _delete(banco, tipo):
        alvo = input("Alvo a ser deletado: ")
        query = "MATCH (p:" + tipo + ")" "WHERE p.nome = $alvo " "DELETE p"
        banco.run(query, alvo=alvo)

    @staticmethod
    def _delete_relacao(banco, tipo):
        alvo = input("Alvo a ser deletado com alguma relação: ")
        query = (
            "MATCH (p:" + tipo + ")-[relacao:%]->()"
            "WHERE p.nome = $alvo "
            "DELETE p, relacao"
        )
        banco.run(query, alvo=alvo)

    @staticmethod
    def _criar_objeto(banco, tipo):
        if tipo == "pessoa" or tipo == "vendedor":
            query = (
                "CREATE (objeto:"
                + tipo
                + " { email: $email_pessoa , idade: $idade_pessoa , nome: $nome_pessoa })"
            )
            email_pessoa = input("Digite o email: ")
            nome_pessoa = input("Nome pessoa: ")
            idade_pessoa = input("Idade pessoa: ")
            resultado = banco.run(
                query,
                email_pessoa=email_pessoa,
                idade_pessoa=idade_pessoa,
                nome_pessoa=nome_pessoa,
            )
            return [
                {"objeto": row["objeto"]["email"]["idade"]["nome"]} for row in resultado
            ]
        elif tipo == "produto":
            query = (
                "CREATE (objeto:"
                + tipo
                + " {  valor: $valor_produto , quantidade: $quantidade_produto , nome: $nome_produto })"
            )
            nome_produto = input("Nome do produto: ")
            valor_produto = input("Valor do produto: ")
            quantidade_produto = input("Quantidade de produtos: ")
            resultado = banco.run(
                query,
                nome_produto=nome_produto,
                quantidade_produto=quantidade_produto,
                valor_produto=valor_produto,
            )
            return [
                {"objeto": row["objeto"]["valor"]["quantidade"]["nome"]}
                for row in resultado
            ]

    @staticmethod
    def _vendedor_produto(banco):
        vendedor_nome = input("Qual é o nome da pessoa?: ")
        produto_nome = input("Qual produto ela vendeu?: ")
        query = (
            "MATCH (p2:vendedor)"
            "WHERE p2.nome = $vendedor_nome "
            "MATCH (p1:produto)"
            "WHERE p1.nome = $produto_nome "
            "CREATE (p2)-[:vende]->(p1) "
            "RETURN p1, p2"
        )
        result = banco.run(
            query, vendedor_nome=vendedor_nome, produto_nome=produto_nome
        )
        try:
            return [
                {"p1": row["p1"]["nome"], "p2": row["p2"]["nome"]} for row in result
            ]
        except ServiceUnavailable as exception:
            logging.error(
                "{query} raised an error: \n {exception}".format(
                    query=query, exception=exception
                )
            )
            raise

    @staticmethod
    def _pessoa_produto(banco):
        pessoa_nome = input("Qual é o nome da pessoa?: ")
        produto_nome = input("Qual produto ela consumiu?: ")
        tipo_oferta = input("Ação tomada: ")
        query = (
            "MATCH (p2:pessoa)"
            "WHERE p2.nome = $pessoa_nome "
            "MATCH (p1:produto)"
            "WHERE p1.nome = $produto_nome "
            "CREATE (p2)-[:" + tipo_oferta + "]->(p1) "
            "RETURN p1, p2"
        )
        result = banco.run(query, pessoa_nome=pessoa_nome, produto_nome=produto_nome)
        try:
            return [
                {"p1": row["p1"]["nome"], "p2": row["p2"]["nome"]} for row in result
            ]
        except ServiceUnavailable as exception:
            logging.error(
                "{query} raised an error: \n {exception}".format(
                    query=query, exception=exception
                )
            )
            raise

    @staticmethod
    def _querys_produto_vendedor(banco, query):
        findQuery = query
        pessoa_nome = input("Nome: ")
        query = (
            "MATCH (p:" + findQuery + ") "
            "WHERE p.nome = $pessoa_nome "
            "RETURN p.nome as nome, p.email as email, p.idade as idade"
        )
        result = banco.run(query, pessoa_nome=pessoa_nome)
        return [[row["email"], row["nome"], row["idade"]] for row in result]

    @staticmethod
    def _querys_produtos(banco, query):
        query = "produto"
        produto_nome = input("Nome: ")
        query = (
            "MATCH (p:" + query + ") "
            "WHERE p.nome = $produto_nome "
            "RETURN p.nome as nome, p.quantidade as quantidade, p.valor as valor"
        )
        result = banco.run(query, produto_nome=produto_nome)
        return [[row["quantidade"], row["nome"], row["valor"]] for row in result]

    @staticmethod
    def _querys(banco, query):
        if query == "produto" or query == "vendedor":
            query = (
                "MATCH (p:" + query + ") "
                "RETURN p.nome as nome, p.email as email, p.idade as idade"
            )
            result = banco.run(query)
            return [[row["email"], row["nome"], row["idade"]] for row in result]
        elif query == "produto":
            query = query
            query = (
                "MATCH (p:" + query + ") "
                "RETURN p.nome as nome, p.quantidade as quantidade, p.valor as valor"
            )
            result = banco.run(query)
            return [[row["quantidade"], row["nome"], row["valor"]] for row in result]


if __name__ == "__main__":
    uri = "neo4j+s://22a7df4a.databases.neo4j.io"
    user = "neo4j"
    password = "z_cq_UHdjSPtczdU0dw-2US11sFUyw9VhvXG7QywsZQ"
    app = App(uri, user, password)
    on = True
    while on:
        print(
            """
        1 - Criar Objetos
        2 - Criar Relações
        3 - Queys
        4 - Deletar
        5 - Atualizar
        6 / X - Sair
        """
        )
        escolha = input("Escolha: ")
        if escolha == "1":
            app.criar_objeto()
        if escolha == "2":
            app.relacionamentos()
        if escolha == "3":
            app.querys()
        if escolha == "4":
            app.delete()
        if escolha == "5":
            app.atualiazr()
        if escolha == "x" or escolha == "6":
            on = False
            app.close()
